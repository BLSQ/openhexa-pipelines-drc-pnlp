PROJECT_BASE_PATH <- '/home/hexa/workspace/pnlp-tdb-pipeline/'

################################################################
agg_helper <- function(x) {
    return(sum(x, na.rm=TRUE))
}
################################################################

################################################################
format_time_period <- function(df, time_period, data_type='DE'){
    
    df <- data.frame(df)
    
    df <- df %>%
      mutate(PERIOD = as.character(PERIOD)) %>%
      mutate(PERIOD = as.yearmon(PERIOD, format='%Y%m')) %>%
      mutate(Annee = as.numeric(substring(PERIOD, 4, 8))) %>%
      mutate(Filtre_time = 'Mensuel') %>%
      mutate(Agg_period = as.character(PERIOD)) # Yearmon throwing strange errors
      
    if (time_period == 'trimestre') {
        df = df %>%
             mutate(Agg_period = case_when(
                                     grepl("Jan|Feb|Mar", PERIOD) ~ 'T1',
                                     grepl("Apr|May|Jun", PERIOD) ~ 'T2',
                                     grepl("Jul|Aug|Sep", PERIOD) ~ 'T3',
                                     grepl("Oct|Nov|Dec", PERIOD) ~ 'T4')) %>%
             mutate(Filtre_time = 'Trimestre')
    } else if(time_period == 'semestre') {
        df = df %>%
             mutate(Agg_period = case_when(grepl("Jan|Feb|Mar|Apr|May|Jun", 
                                                 PERIOD) ~ 'S1',
                                           grepl("Jul|Aug|Sep|Oct|Nov|Dec", 
                                                 PERIOD) ~ 'S2')) %>%
             mutate(Filtre_time = 'Semestre')


    } else if(time_period == 'annee') {
        df = df %>% 
             mutate(Agg_period = Annee) %>%
             mutate(Filtre_time = 'Année')
    }
        
    if(data_type == 'DE'){
        
        df = df %>% pivot_wider(id_cols = c(Annee, Filtre_time,
                                            COC_NAME,
                                            LEVEL_5_UID), 
                                 names_from = c(Agg_period, DE_UID), 
                                 values_from = VALUE,
                                 values_fn = agg_helper) %>% 

                    pivot_longer(cols = !c(Annee, Filtre_time,
                                                    COC_NAME,
                                                    LEVEL_5_UID),
                                          names_to = c('Agg_period', 'DE_UID'),
                                          names_sep = "_")
    } else if (data_type == 'CP') { # reporting datasets
        n_months <- length(unique(df$PERIOD))
        
        df = df %>% pivot_wider(id_cols = c(Annee, 
                                            REPORTING_TYPE, Filtre_time,
                                            LEVEL_5_UID), 
                                 names_from = c(Agg_period, DS_UID), 
                                 values_from = VALUE,
                                 values_fn = agg_helper) %>% 

                    pivot_longer(cols = !c(Annee,
                                           REPORTING_TYPE, Filtre_time,
                                           LEVEL_5_UID),
                                          names_to = c('Agg_period', 'DS_UID'),
                                          names_sep = "_")
        
        # slightly hacky fix to partial year problem
        # TODO: come up with a better solution here
        if(unique(df$Annee) == 2021) {
            l <- list('mois' = 100, 'trimestre' = 300, 
                      'semestre' = 100 * n_months, 'annee' = 100 * n_months)
            
        } else {            
            l <- list('mois' = 100, 'trimestre' = 300, 
                      'semestre' = 600, 'annee' = 1200)
        }
        
#         df = df %>% mutate(value = value / l[[time_period]])
        
    }
        
    else { # indicator datasets
        df = df %>% pivot_wider(id_cols = c(Annee, Filtre_time,
                                            LEVEL_5_UID), 
                                 names_from = c(Agg_period, IND_UID), 
                                 values_from = VALUE,
                                 values_fn = agg_helper) %>% 

                    pivot_longer(cols = !c(Annee, Filtre_time,
                                           LEVEL_5_UID),
                                          names_to = c('Agg_period', 'IND_UID'),
                                          names_sep = "_")
    }
        
    if(time_period %in% c('semestre', 'trimestre')){
        df <- df %>% mutate(Agg_period = paste(Agg_period, Annee))
    }
        
    # ous <- fread(paste0(PROJECT_BASE_PATH,
    #                        'data/raw/routine/2023/Q1/metadata/organisation_units.csv'))
    
    ous <- fread(paste0(PROJECT_BASE_PATH, 'data/metadata/organisation_units.csv'))
        
    fosas <- ous[ou_level == 5]
    fosas[, geometry := NULL]
    
    fosas[, paste0('parent_level_', 1:4) := tstrsplit(path, '/', keep = 2:5)]
    
    fosas <- merge(fosas, ous[,.(parent_level_4 = ou_uid, parent_level_4_name = ou_name)], by = 'parent_level_4')
    fosas <- merge(fosas, ous[,.(parent_level_3 = ou_uid, parent_level_3_name = ou_name)], by = 'parent_level_3')
    fosas <- merge(fosas, ous[,.(parent_level_2 = ou_uid, parent_level_2_name = ou_name)], by = 'parent_level_2')
    fosas <- merge(fosas, ous[,.(parent_level_1 = ou_uid, parent_level_1_name = ou_name)], by = 'parent_level_1')

    fosas <- fosas[, .(OU_UID = ou_uid,
                       OU_NAME = ou_name,
                       LEVEL_5_UID = ou_uid,
                       LEVEL_5_NAME = ou_name,
                       LEVEL_4_UID = parent_level_4,
                       LEVEL_4_NAME = parent_level_4_name,
                       LEVEL_3_UID = parent_level_3,
                       LEVEL_3_NAME = parent_level_3_name,
                       LEVEL_2_UID = parent_level_2,
                       LEVEL_2_NAME = parent_level_2_name,
                       LEVEL_1_UID = parent_level_1,
                       LEVEL_1_NAME = parent_level_1_name)]
    
    df <- merge(df, fosas, by = c('LEVEL_5_UID'))
        
    return(df)
}
################################################################

################################################################
calculer_indicateur <- function(df, indicateur_name, numerator, 
                                denominator, valeurs_classement, population=FALSE) {
    
    df = data.frame(df)
    
    # subset to DEs we're interested in
    if(!population){
        df <- df %>%
             filter(DE_UID %in% c(denominator, numerator)) %>%
             select(LEVEL_5_NAME, LEVEL_5_UID,
                   LEVEL_4_NAME, LEVEL_4_UID,
                   LEVEL_3_NAME, LEVEL_3_UID,
                   LEVEL_2_NAME, LEVEL_2_UID,
                   LEVEL_1_NAME, LEVEL_1_UID,
                   COC_NAME, 
                   DE_UID, value, Annee,
                   Agg_period, Filtre_time)
        
        if (indicateur_name == 'MILD_CPS') {
            df <- rbind(filter(df, 
                               DE_UID == 'pak21wvkWJC' & 
                               (COC_NAME == '0 et 11 mois, Avancé' | 
                                COC_NAME == '0 et 11 mois, Mobile' | 
                                COC_NAME == '0 et 11 mois, Fixe')),
                        filter(df,
                               DE_UID == 'QafSfDEL1Ku' & COC_NAME == '6 et 11 mois'))
        }
        
        # put each DE of interest into its own column
        df <- df %>%
              pivot_wider(id_cols = c(Agg_period, LEVEL_5_NAME, LEVEL_5_UID,
                                    LEVEL_4_NAME, LEVEL_4_UID,
                                    LEVEL_3_NAME, LEVEL_3_UID,
                                    LEVEL_2_NAME, LEVEL_2_UID,
                                    LEVEL_1_NAME, LEVEL_1_UID,
                                    COC_NAME, Annee, 
                                    Filtre_time), 
                          names_from = DE_UID, values_from = value)
    } else {
        df <- df %>%
             filter(DE_UID %in% c(denominator, numerator)) %>%
             select(LEVEL_4_NAME, LEVEL_4_UID,
                   LEVEL_3_NAME, LEVEL_3_UID,
                   LEVEL_2_NAME, LEVEL_2_UID,
                   LEVEL_1_NAME, LEVEL_1_UID,
                   COC_NAME, 
                   DE_UID, value, Annee,
                   Agg_period, Filtre_time)
        
        df <- df %>%
              pivot_wider(id_cols = c(Agg_period,
                                    LEVEL_4_NAME, LEVEL_4_UID,
                                    LEVEL_3_NAME, LEVEL_3_UID,
                                    LEVEL_2_NAME, LEVEL_2_UID,
                                    LEVEL_1_NAME, LEVEL_1_UID,
                                    COC_NAME, Annee, 
                                    Filtre_time), 
                          names_from = DE_UID, 
                          values_from = value,
                          values_fn = agg_helper)
    }



    # sum component elements of numerator and denominator at FOSA-month-COC level
    df <- df %>%
          mutate(num_sum = rowSums(.[grep(paste(numerator, collapse='|'), 
                                          names(.))],
                                   na.rm = TRUE)) %>%
          mutate(den_sum = rowSums(.[grep(paste(denominator, collapse='|'), 
                                          names(.))],
                                   na.rm = TRUE))
    
    
    agg_combined <- df %>%
                group_by(LEVEL_1_NAME, LEVEL_1_UID,
                         LEVEL_2_NAME, LEVEL_2_UID,
                         LEVEL_3_NAME, LEVEL_3_UID,
                         LEVEL_4_NAME, LEVEL_4_UID,
                         Annee, Agg_period, Filtre_time) %>%
                summarize(num_sum = sum(num_sum),
                         den_sum = sum(den_sum)) %>%
                ungroup()

    
    agg_combined <- agg_combined %>%
                mutate(den_sum = ifelse(den_sum == 0, NA, den_sum)) %>%
                mutate(num_sum = ifelse(num_sum == 0, NA, num_sum)) %>%
                mutate(Indicateur_num = num_sum) %>%
                mutate(Indicateur_den = den_sum) %>%
                mutate(Indicateur_name = indicateur_name)

    # format output
    agg_combined <- agg_combined %>%
                    select(Agg_period, Annee, Filtre_time,
                           Indicateur_name,
                           Indicateur_num, Indicateur_den,
                           LEVEL_1_NAME, LEVEL_1_UID,
                           LEVEL_2_NAME, LEVEL_2_UID,
                           LEVEL_3_NAME, LEVEL_3_UID,
                           LEVEL_4_NAME, LEVEL_4_UID)
                           # LEVEL_4_NAME, LEVEL_4_UID
                           # LEVEL_5_NAME, LEVEL_5_UID
    
    return(agg_combined)
}
################################################################

################################################################
aggreger_reporting <- function(df, reporting_type, 
                               reporting_uid, indicateur_name,
                               valeurs_classement) {
    
    df = data.frame(df)
    
    # load org unit group depending on indicator
    if (indicateur_name %like% 'hosp') {
        ou_group_name <- 'D - Service Hôpital'
    } else if (indicateur_name %like% 'second') {
        ou_group_name <- 'B - Services Secondaires'
    } else {
        ou_group_name <- 'A - Services de Base'
    }
    
    groups <- fread(paste0(PROJECT_BASE_PATH,
                           'data/metadata/organisation_unit_groups.csv'))
    
    ou_list <- groups[oug_shortname == ou_group_name, c(org_units)]
    ou_list <- strsplit(ou_list, ';')[[1]]
    
    # remplacer les NAs par 0
    df <- df %>%
            mutate(value = ifelse(is.na(value), 0, value))
    
    # subset to data we're interested in
    df <- df %>%
         filter(DS_UID == reporting_uid & 
                REPORTING_TYPE == reporting_type &
                LEVEL_5_UID %in% ou_list) %>%
         select(LEVEL_5_NAME, LEVEL_5_UID,
               LEVEL_4_NAME, LEVEL_4_UID,
               LEVEL_3_NAME, LEVEL_3_UID,
               LEVEL_2_NAME, LEVEL_2_UID,
               LEVEL_1_NAME, LEVEL_1_UID,
               DS_UID, value, Annee,
               Agg_period, Filtre_time)

    # division + ajouter le nom de l'indicateur
    
    agg_combined <- df %>%
            group_by(LEVEL_1_NAME, LEVEL_1_UID,
                     LEVEL_2_NAME, LEVEL_2_UID,
                     LEVEL_3_NAME, LEVEL_3_UID,
                     LEVEL_4_NAME, LEVEL_4_UID,
                     Annee, Agg_period, Filtre_time) %>%
            summarize(num_sum = sum(value),
                      den_sum = 100 * n()) %>%
            ungroup()
    
    agg_combined <- agg_combined %>%
                    mutate(Indicateur_num = num_sum) %>%
                    mutate(Indicateur_den = den_sum) %>%
                    mutate(Indicateur_name = indicateur_name)

    # format output
    agg_combined <- agg_combined %>%
                    select(Agg_period, Annee, Filtre_time,
                           Indicateur_name,
                           Indicateur_num, Indicateur_den,
                           LEVEL_1_NAME, LEVEL_1_UID,
                           LEVEL_2_NAME, LEVEL_2_UID,
                           LEVEL_3_NAME, LEVEL_3_UID,
                           LEVEL_4_NAME, LEVEL_4_UID)
    
    return(agg_combined)
}

sommer_indicateur <- function(df, indicateur_name, element_ids) {
    
    df = data.frame(df)
    
    df <- df %>%
         filter(DE_UID %in% element_ids) %>%
         select(LEVEL_4_NAME, LEVEL_4_UID,
               LEVEL_3_NAME, LEVEL_3_UID,
               LEVEL_2_NAME, LEVEL_2_UID,
               LEVEL_1_NAME, LEVEL_1_UID,
               COC_NAME, 
               DE_UID, value, Annee,
               Agg_period, Filtre_time)

    df <- df %>%
          pivot_wider(id_cols = c(Agg_period,
                                LEVEL_4_NAME, LEVEL_4_UID,
                                LEVEL_3_NAME, LEVEL_3_UID,
                                LEVEL_2_NAME, LEVEL_2_UID,
                                LEVEL_1_NAME, LEVEL_1_UID,
                                COC_NAME, Annee, 
                                Filtre_time), 
                      names_from = DE_UID, 
                      values_from = value,
                      values_fn = agg_helper)



    # sum component elements of numerator and denominator at FOSA-month-COC level
    df <- df %>%
          mutate(ind_sum = rowSums(.[grep(paste(element_ids, collapse='|'), 
                                          names(.))],
                                   na.rm = TRUE))
    
    agg_combined <- df %>%
        group_by(LEVEL_1_NAME, LEVEL_1_UID,
                 LEVEL_2_NAME, LEVEL_2_UID,
                 LEVEL_3_NAME, LEVEL_3_UID,
                 LEVEL_4_NAME, LEVEL_4_UID,
                 Annee, Agg_period, Filtre_time) %>%
        summarize(num_sum = sum(ind_sum)) %>%
        ungroup()

    agg_combined <- agg_combined %>%
                    mutate(Indicateur_num = num_sum) %>%
                    mutate(Indicateur_den = 1) %>%
                    mutate(Indicateur_name = indicateur_name)
                    

    # format output
    agg_combined <- agg_combined %>%
                    select(Agg_period, Annee, Filtre_time,
                           Indicateur_name,
                           Indicateur_num, Indicateur_den,
                           LEVEL_1_NAME, LEVEL_1_UID,
                           LEVEL_2_NAME, LEVEL_2_UID,
                           LEVEL_3_NAME, LEVEL_3_UID,
                           LEVEL_4_NAME, LEVEL_4_UID)
    
    return(agg_combined)
}
################################