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
        },
        "KISENSO": {
            "PAIX": "DE LA PAIX",
        },
    }
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
relevant_info_cols = ["date", "location_id"]
relevant_ewars_forms_cols = relevant_info_cols + relevant_malaria_cols
ewars_formated_cols = ["variable", "value", "epi_week", "dhis2_level_4_id"]
