agg_helper <- function(x) {
    return(sum(x, na.rm=FALSE))
}

# format_time_period <- function(df, time_period, rupture=FALSE){
    
#     df <- data.frame(df)
    
#     df <- df %>%
#       mutate(PERIOD = as.character(PERIOD)) %>%
#       mutate(PERIOD = as.yearmon(PERIOD, format='%Y%m')) %>%
#       mutate(Annee = as.numeric(substring(PERIOD, 4, 8))) %>%
#       mutate(Filtre_time = 'Mensuel') %>%
#       mutate(Agg_period = as.character(PERIOD)) # Yearmon throwing strange errors
      
#     if (time_period == 'trimestre') {
#         df = df %>%
#              mutate(Agg_period = case_when(
#                                      grepl("Jan|Feb|Mar", PERIOD) ~ 'T1',
#                                      grepl("Apr|May|Jun", PERIOD) ~ 'T2',
#                                      grepl("Jul|Aug|Sep", PERIOD) ~ 'T3',
#                                      grepl("Oct|Nov|Dec", PERIOD) ~ 'T4')) %>%
#              mutate(Filtre_time = 'Trimestre')
#     } else if(time_period == 'semestre') {
#         df = df %>%
#              mutate(Agg_period = case_when(grepl("Jan|Feb|Mar|Apr|May|Jun", 
#                                                  PERIOD) ~ 'S1',
#                                            grepl("Jul|Aug|Sep|Oct|Nov|Dec", 
#                                                  PERIOD) ~ 'S2')) %>%
#              mutate(Filtre_time = 'Semestre')


#     } else if(time_period == 'annee') {
#         df = df %>% 
#              mutate(Agg_period = Annee) %>%
#              mutate(Filtre_time = 'Année')
#     }
        
#     df = df %>% pivot_wider(id_cols = c(OU_NAME, Annee, Filtre_time,
#                                         COC_NAME,
#                                         LEVEL_5_NAME, LEVEL_5_UID,
#                                         LEVEL_4_NAME, LEVEL_4_UID,
#                                         LEVEL_3_NAME, LEVEL_3_UID,
#                                         LEVEL_2_NAME, LEVEL_2_UID,
#                                         LEVEL_1_NAME, LEVEL_1_UID), 
#                              names_from = c(Agg_period, DE_UID, DE_NAME), 
#                              values_from = VALUE,
#                              values_fn = agg_helper) %>% 

#                 pivot_longer(cols = !c(OU_NAME, Annee, Filtre_time,
#                                                 COC_NAME,
#                                                 LEVEL_5_NAME, LEVEL_5_UID,
#                                                 LEVEL_4_NAME, LEVEL_4_UID,
#                                                 LEVEL_3_NAME, LEVEL_3_UID,
#                                                 LEVEL_2_NAME, LEVEL_2_UID,
#                                                 LEVEL_1_NAME, LEVEL_1_UID),
#                                       names_to = c('Agg_period', 'DE_UID', 'DE_NAME'),
#                                       names_sep = "_")
    
#     if(time_period %in% c('semestre', 'trimestre')){
#         df <- df %>% mutate(Agg_period = paste(Agg_period, Annee))
#     }
    
#     return(df)
# }

format_time_period <- function(df){
    
    df <- copy(df)
    setDT(df)
    
    
    de.period.ou.combos <- expand.grid(OU_UID = unique(df$OU_UID),
                                       PERIOD = unique(df$PERIOD),
                                       DE_UID = unique(df$DE_UID))
    
    de.coc.combos <- unique(df[,.(DE_UID, COC_UID)])
    
    rect.df <- merge(de.period.ou.combos, de.coc.combos,
                     by = 'DE_UID', allow.cartesian = TRUE)
    
    rect.df <- merge(rect.df, df[,.(OU_UID, PERIOD, DE_UID, COC_UID, VALUE)],
                     all.x = TRUE)
    
    de.coc.meta <- unique(df[,.(DE_UID, DE_NAME, COC_UID, COC_NAME)])
    ou.meta <- unique(df[,.(LEVEL_1_UID, LEVEL_1_NAME,	
                            LEVEL_2_UID, LEVEL_2_NAME,	
                            LEVEL_3_UID, LEVEL_3_NAME,	
                            LEVEL_4_UID, LEVEL_4_NAME,	
                            LEVEL_5_UID, LEVEL_5_NAME,	
                            OU_NAME, OU_UID)])
    
    rect.df <- merge(rect.df, de.coc.meta, by = c('COC_UID', 'DE_UID'))
    rect.df <- merge(rect.df, ou.meta, by = 'OU_UID')
    
    setDT(rect.df)
    
    rect.df[, PERIOD := as.character(PERIOD)]
    rect.df[, Agg_period := as.character(as.yearmon(PERIOD, format='%Y%m'))]
    rect.df[, Annee := as.numeric(substring(PERIOD, 1, 4))]
    rect.df[, Filtre_time := 'Mensuel'] # legacy
      
    rect.df <- rect.df[,.(OU_NAME, Annee,
                       Filtre_time, COC_NAME,
                       LEVEL_5_NAME, LEVEL_5_UID,
                       LEVEL_4_NAME, LEVEL_4_UID,
                       LEVEL_3_NAME, LEVEL_3_UID,
                       LEVEL_2_NAME, LEVEL_2_UID,
                       LEVEL_1_NAME, LEVEL_1_UID,
                       Agg_period,
                       DE_UID, DE_NAME, value = VALUE)]
    
    return(rect.df)
}



calculer_indicateur <- function(df, indicateur_name) {
    
    df = data.table(df)
    
    # subset to DEs we're interested in
    if(indicateur_name == 'Stock_Disp'){
        df <- df[COC_NAME == 'Stock Disponible Utilisable-SDU' & DE_NAME %like% 'Zero or Positive Int'] # bit of a hack
    }
    else if (indicateur_name == 'ACT_consommé'){
        df <- df[DE_NAME %like% 'Artemether|sunate' & COC_NAME == 'Quantité consommée']
        df[, DE_NAME := 'Groupe ACT']
    }
    else if (indicateur_name == 'Mois_stock') {
        df <- df[COC_NAME == 'MSD' & DE_NAME %like% 'Number']
    } 
    else if (indicateur_name == 'CMM') {        
        df <- df[COC_NAME == 'CMM' & DE_NAME %like% 'Zero or Positive Int'] # select COC CMM for these group of DE
    }
    else {
        stop("Error indicator selection not supported")
    }

    # aire de santé
    agg_combined <- df[, .(value = sum(value, na.rm = TRUE)), 
                       by = .(Agg_period, DE_NAME,
                              LEVEL_4_NAME, LEVEL_4_UID,
                              LEVEL_3_NAME, LEVEL_3_UID,
                              LEVEL_2_NAME, LEVEL_2_UID,
                              LEVEL_1_NAME, LEVEL_1_UID,
                              Annee, Filtre_time)]
    
    agg_combined[, Indicateur_num := value]
    agg_combined[, Indicateur_den := 1]
    agg_combined[, Indicateur_name := indicateur_name]
    
    # format output
    agg_combined <- agg_combined[,.(Agg_period, Annee, Filtre_time,
                                     Indicateur_name, DE_NAME, 
                                     Indicateur_num, Indicateur_den,
                                     LEVEL_1_NAME, LEVEL_1_UID,
                                     LEVEL_2_NAME, LEVEL_2_UID,
                                     LEVEL_3_NAME, LEVEL_3_UID,
                                     LEVEL_4_NAME, LEVEL_4_UID)]
    
    return(agg_combined)

}


pct_sans_rupture <- function(df, indicateur_name) {
    
    df = data.table(df)
    
    # subset to DEs we're interested in
    df <- df[COC_NAME == 'Nbre de jours de rupture de stock' & DE_NAME %like% "Zero or Positive Int"] # bit of a hack
    df[, no_rupture := FALSE][(value == 0) | (is.na(value)), no_rupture := TRUE]
    
    if(indicateur_name  == 'Pct_FOSA_Sans_Rupture_ACT') {
        # subset to relevant products
        df <- df[DE_NAME %like% 'Artemether|sunate']
        
        df <- df[, .(no_rupture = (sum(no_rupture) == .N)),
               by = .(Agg_period,
                      LEVEL_5_NAME, LEVEL_5_UID,
                      LEVEL_4_NAME, LEVEL_4_UID,
                      LEVEL_3_NAME, LEVEL_3_UID,
                      LEVEL_2_NAME, LEVEL_2_UID,
                      LEVEL_1_NAME, LEVEL_1_UID,
                      Annee, Filtre_time)]
        df[, DE_NAME := 'Groupe ACT']
    }
    else if (indicateur_name == 'Pct_FOSA_Sans_Rupture_Tous_Produits') {
             df <- df[, .(no_rupture = (sum(no_rupture) == .N)),
                by = .(Agg_period,
                      LEVEL_5_NAME, LEVEL_5_UID,
                      LEVEL_4_NAME, LEVEL_4_UID,
                      LEVEL_3_NAME, LEVEL_3_UID,
                      LEVEL_2_NAME, LEVEL_2_UID,
                      LEVEL_1_NAME, LEVEL_1_UID,
                      Annee, Filtre_time)]
             df[, DE_NAME := 'Tous les produits traceurs']
    }
    else{
    }
    
    # aire de santé
    agg_combined <- df[, .(Indicateur_num = sum(no_rupture),
                           Indicateur_den = .N), 
                       by = .(Agg_period, DE_NAME,
                              LEVEL_4_NAME, LEVEL_4_UID,
                              LEVEL_3_NAME, LEVEL_3_UID,
                              LEVEL_2_NAME, LEVEL_2_UID,
                              LEVEL_1_NAME, LEVEL_1_UID,
                              Annee, Filtre_time)]
    
    if(indicateur_name %in% c('Pct_FOSA_Sans_Rupture_ACT', 
                              'Pct_FOSA_Sans_Rupture_Tous_Produits')) {
        agg_combined[, Indicateur_name := 'Pct_FOSA_Sans_Rupture']
    }
    else {
        agg_combined[, Indicateur_name := 'Pct_FOSA_Sans_Rupture']
    }           
    
    # format output
    agg_combined <- agg_combined[,.(Agg_period, Annee, Filtre_time,
                                     Indicateur_name, DE_NAME, 
                                     Indicateur_num, Indicateur_den,
                                     LEVEL_1_NAME, LEVEL_1_UID,
                                     LEVEL_2_NAME, LEVEL_2_UID,
                                     LEVEL_3_NAME, LEVEL_3_UID,
                                     LEVEL_4_NAME, LEVEL_4_UID)]
    
    return(agg_combined)
}