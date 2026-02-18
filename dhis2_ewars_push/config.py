thresholds_match = [90, 80, 60]
list_levels = [
    "level_1",
    "level_2",
    "level_3",
    "level_4",
]
numeric_replacements = {r"\bI\b": "1", r"\bII\b": "2", r"\bIII\b": "3", r"\bIV\b": "4", r"\bV\b": "5"}
ewars_level3_replacements = {"TSHOPO": {"MAKISO": "MAKISO KISANGANI"}, "ITURI": {"NYAKUNDA": "NYANKUNDE"}}
ewars_level4_replacements = {
    "KINSHASA": {
        "KOKOLO": {
            "NDOLO PRISON": "NDOLO",
            "STP": "STP MAKELELE",
            "TSHATSHI": "TSHATSHI HOPITAL MILITAIRE DE LA GARDE REPUBLICAINE",
        },
        "KISENSO": {
            "PAIX": "DE LA PAIX",
        },
        "KALAMU 2": {
            "YOLO SUD 4": "YOLO-SUD 4",
        },
        "MALUKU 1": {
            "KINGANKATI  1": "KINGANKATI",
        },
        "N'SELE": {"MBANGUMBAMU": "UNKNOWN"},  # We dont want to match it wrongly to BAHUMBU 1
    },
    "KWILU": {
        "KIKWIT NORD": {"MARCHE": "MARCHE KIKWIT 3"},
        "MUNGINDU": {"INERA": "INERA KIYAKA"},
        "MUKEDI": {"DONGO": "DONGO SELENGE"},
        "MOKALA": {"PANU 417": "417"},
    },
    "ITURI": {"TCHOMIA": {"KASENYI": "KASENYI CENTRE"}},
    "SUD KIVU": {
        "MUBUMBANO": {"MUZINZI": "MUZINZI DE MUBUMBANO", "IBANDA": "IBANDA DE MUBUMBANO"},
        "NUNDU": {"IAMBA": "IAMBA /MAKOBOLA2"},
        "KIMBI LULENGE": {"SUNGWE": "SUNGWE DE KIMBI LULENGE"},
    },
    "KONGO CENTRAL": {
        "BOKO KIVULU": {"TEMBO": "UNKNOWN"},  # We dont want to match it wrongly to NDEMBO
        "GOMBE MATADI": {"KINKUZU": "UNKNOWN"},  # We dont want to match it wrongly to NKAZU
    },
    "NORD KIVU": {
        "KIBUA": {
            "KAILENGE": "UNKNOWN",  # We dont want to match it wrongly to KILUNGU
            "MIKUMBI": "UNKNOWN",  # We dont want to match it wrongly to MACHUMBI
            "MIRUTA": "UNKNOWN",  # We dont want to match it wrongly to MWITWA
        },
        "RUTSHURU": {
            "KATORO": "UNKNOWN",  # We dont want to match it wrongly to KAKOMERO
        },
    },
    "LOMAMI": {
        "MWENE DITU": {"TSHISHINJI": "UNKNOWN"},  # We dont want to match it wrongly to CINSIHNJI
    },
}
ewars_level3_replacements_somelevel4 = {
    "ITURI": [
        {"NYANKUNDE": "NYARAMBE"},
        [
            "JUPADROGO",
            "JUPAGWEYI",
            "AGUDI USOKE",
            "PUNDIGA",
            "ABIRA AREJU",
            "AFOYO",
            "ALLA CECA",
            "AMBAKI",
            "ANYIKO",
            "DJEGU",
            "KPANYI",
            "LELO",
            "MAHAGI PORT CECCA",
            "MAHAGI PORT ETAT",
            "NYALEBBE",
            "NYARAMBE MISSION",
            "PAKULE THERANGO",
            "PATHOLE",
            "UGWILO",
            "LENJU",
            "PAJAW",
            "ANG'ABA",
            "KASENGU",
        ],
    ]
}
form_id = 176
relevant_malaria_cols = [
    "paludisme_suspect_s1_paludisme_suspect-cas_0-11mois",
    "paludisme_suspect_s1_paludisme_suspect-cas_12-59mois",
    "paludisme_suspect_s1_paludisme_suspect-cas_5-15ans",
    "paludisme_suspect_s1_paludisme_suspect-cas_>15ans",
    "paludisme_suspect_s1_paludisme_suspect-deces_0-11mois",
    "paludisme_suspect_s1_paludisme_suspect-deces_12-59mois",
    "paludisme_suspect_s1_paludisme_suspect-deces_5-15ans",
    "paludisme_suspect_s1_paludisme_suspect-deces_>15ans",
    "paludisme_confirm_**_s1_paludisme_confirm-cas_0-11mois",
    "paludisme_confirm_**_s1_paludisme_confirm-cas_12-59mois",
    "paludisme_confirm_**_s1_paludisme_confirm-cas_5-15ans",
    "paludisme_confirm_**_s1_paludisme_confirm-cas_>15ans",
    "paludisme_confirm_**_s1_paludisme_confirm-deces_0-11mois",
    "paludisme_confirm_**_s1_paludisme_confirm-deces_12-59mois",
    "paludisme_confirm_**_s1_paludisme_confirm-deces_5-15ans",
    "paludisme_confirm_**_s1_paludisme_confirm-deces_>15ans",
]
relevant_info_cols = [
    "date",
    "location_id",
]
relevant_level_cols = [
    "ewars_level_4_name_cleaned",
    "dhis2_level_4_name_cleaned",
    "score_level_4",
    "dhis2_level_4_id",
    "ewars_level_3_name_cleaned",
    "dhis2_level_3_name_cleaned",
    "score_level_3",
    "ewars_level_3_id",
    "dhis2_level_3_id",
    "ewars_level_2_name_cleaned",
    "dhis2_level_2_name_cleaned",
    "score_level_2",
    "ewars_level_2_id",
    "dhis2_level_2_id",
    "ewars_level_1_name_cleaned",
    "dhis2_level_1_name_cleaned",
    "score_level_1",
    "ewars_level_1_id",
    "dhis2_level_1_id",
]
relevant_ewars_forms_cols = relevant_info_cols + relevant_malaria_cols
ewars_formated_cols = ["variable", "value", "epi_week", "dhis2_level_4_id"]
dict_dE_CoC = relevant_malaria_cols = {
    "paludisme_suspect_s1_paludisme_suspect-cas_0-11mois": ["RvnnOHzyyl5", "LPC4AtW5l03"],
    "paludisme_suspect_s1_paludisme_suspect-cas_12-59mois": ["RvnnOHzyyl5", "jGlFZ1m4k7d"],
    "paludisme_suspect_s1_paludisme_suspect-cas_5-15ans": ["RvnnOHzyyl5", "aOSANPvwJSP"],
    "paludisme_suspect_s1_paludisme_suspect-cas_>15ans": ["RvnnOHzyyl5", "Coa89kwZpQG"],
    "paludisme_suspect_s1_paludisme_suspect-deces_0-11mois": ["kVj09UWaJLJ", "LPC4AtW5l03"],
    "paludisme_suspect_s1_paludisme_suspect-deces_12-59mois": ["kVj09UWaJLJ", "jGlFZ1m4k7d"],
    "paludisme_suspect_s1_paludisme_suspect-deces_5-15ans": ["kVj09UWaJLJ", "aOSANPvwJSP"],
    "paludisme_suspect_s1_paludisme_suspect-deces_>15ans": ["kVj09UWaJLJ", "Coa89kwZpQG"],
    "paludisme_confirm_**_s1_paludisme_confirm-cas_0-11mois": ["jtxlVOyX9UD", "LPC4AtW5l03"],
    "paludisme_confirm_**_s1_paludisme_confirm-cas_12-59mois": ["jtxlVOyX9UD", "jGlFZ1m4k7d"],
    "paludisme_confirm_**_s1_paludisme_confirm-cas_5-15ans": ["jtxlVOyX9UD", "aOSANPvwJSP"],
    "paludisme_confirm_**_s1_paludisme_confirm-cas_>15ans": ["jtxlVOyX9UD", "Coa89kwZpQG"],
    "paludisme_confirm_**_s1_paludisme_confirm-deces_0-11mois": ["hPp0eZ1ahtB", "LPC4AtW5l03"],
    "paludisme_confirm_**_s1_paludisme_confirm-deces_12-59mois": ["hPp0eZ1ahtB", "jGlFZ1m4k7d"],
    "paludisme_confirm_**_s1_paludisme_confirm-deces_5-15ans": ["hPp0eZ1ahtB", "aOSANPvwJSP"],
    "paludisme_confirm_**_s1_paludisme_confirm-deces_>15ans": ["hPp0eZ1ahtB", "Coa89kwZpQG"],
}

list_ewars_to_remove = [
    "MBANGUMBAMU",
    "KAILENGE",
    "MIKUMBI",
    "KINKUZU",
    "KIRIKIRI",
    "TEMBO",
]
