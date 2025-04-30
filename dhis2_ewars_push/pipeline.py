import os
import pandas as pd
from pathlib import Path

# from unidecode import unidecode
from rapidfuzz import fuzz, process
from openhexa.sdk import pipeline, current_run  # workspace


@pipeline("dhis2-ewars-push", name="dhis2_ewars_push")
def dhis2_ewars_push():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """

    ## SETUP ##
    local_wd = Path(os.getcwd()) / "dhis2_ewars_push" / "workspace"
    os.chdir(local_wd)
    ewars_pyramid = pd.read_parquet(local_wd / "dhis2_ewars_push" / "data" / "raw" / "ewars_pyramid.parquet")
    dhis2_pyramid = pd.read_parquet(local_wd / "dhis2_ewars_push" / "data" / "raw" / "dhis2_pyramid.parquet")
    ## SETUP ##

    # workspace.files_path # use this to refer to the workspace environment

    # TASKS
    # --------------------------------------------------------
    # (1) ASYNC: Download and save raw reporting from ewars (task) --> save as parquet (collect new data downloaded - window of months)
    # (2) ASYNC: Download pyramid data from DHIS2  SNIS (task) --> save as parquet
    # (2) ASYNC: Download pyramid data from EWARS (task) --> save as parquet
    # Match data (task) waits for the 2 previous tasks to finish:
    # -build ewars pyramid
    # -RUN names matching process
    # -save ewars mapping table as parquet
    # Load, Format data and push to DHIS2

    ewars_pyramid_processed, matched_names, missing_names = match_pyramid_names(
        df_target=ewars_pyramid,
        df_reference=dhis2_pyramid,
        target_columns=["level_1_name_cleaned", "level_2_name_cleaned", "level_3_name_cleaned", "level_4_name_cleaned"],
        ref_columns=["level_1_name_cleaned", "level_2_name_cleaned", "level_3_name_cleaned", "level_4_name_cleaned"],
        thresholds=[90, 80, 60],
    )

    # Save matched names per level as CSV
    for level, matched_df in matched_names.items():
        print(f"Saving matched names for level {level}...")
        matched_df.to_csv(
            local_wd / "dhis2_ewars_push" / "data" / "processed" / f"{level}_matched_names.csv", index=False
        )  ### CHANGE THE PATH!!

    # NOTE WARNING missing names
    # Save missing names per level as CSV
    for level, missing_df in missing_names.items():
        print(f"Saving missing names for level {level}...")
        if not missing_df.empty:
            missing_sum_path = (
                local_wd / "dhis2_ewars_push" / "data" / "processed" / f"{level}_missing_names.csv"
            )  ### CHANGE THE PATH!!
            missing_df.to_csv(missing_sum_path, index=False)
        current_run.log_warning(f"Missing names for {level}: {len(missing_df)}. Summary saved to {missing_sum_path}.")


def match_pyramid_names(
    df_target: str, df_reference: str, target_columns: list, ref_columns: list, thresholds: int = [90, 80, 60]
):
    """Perform names matching between two dataframes."""

    df_target_copy = df_target.copy()

    # Add corresponding reference columns
    df_target_copy["level_1_name_ref"] = df_reference.level_1_name_cleaned.unique()[0]
    df_target_copy["level_1_id_ref"] = df_reference.level_1_id.unique()[0]

    matches_level = {}
    missing_matches_level = {}

    for i in range(len(thresholds)):
        print(f"Matching level {i + 1}")

        # Match for names per level
        df_target_copy, matched_names, missing_matches = names_matching_per_level(
            df_target=df_target_copy,
            df_reference=df_reference,
            target_parent_column=target_columns[i],
            target_child_column=target_columns[i + 1],
            ref_parent_column=ref_columns[i],
            ref_child_column=ref_columns[i + 1],
            threshold=thresholds[i],
        )
        matches_level[target_columns[i + 1].replace("_cleaned", "")] = matched_names
        missing_matches_level[target_columns[i + 1].replace("_cleaned", "")] = missing_matches

    return df_target_copy, matches_level, missing_matches_level


def names_matching_per_level(
    df_target: str,
    df_reference: str,
    target_parent_column: str,
    target_child_column: str,
    ref_parent_column: str,
    ref_child_column: str,
    threshold: int = 80,
):
    df_target_copy = df_target.copy()

    ### START
    names_not_matched = []
    names_matched = []

    ref_parent_mapping_name = ref_parent_column.replace("_cleaned", "_ref")
    ref_parent_mapping_id = ref_parent_column.replace("_name_cleaned", "_id")

    ref_child_mapping_name = ref_child_column.replace("_cleaned", "_ref")
    ref_child_mapping_id = ref_child_column.replace("_name_cleaned", "_id_ref")

    matching_parents = (
        df_target_copy[[target_parent_column, ref_parent_mapping_name, f"{ref_parent_mapping_id}_ref"]]
        .dropna()
        .drop_duplicates()
    )
    matching_parents.columns = ["target_parent", "ref_parent", "ref_parent_id"]

    # initialize mapping columns
    df_target_copy[ref_child_mapping_name] = None
    df_target_copy[ref_child_mapping_id] = None

    for i, row_parent in matching_parents.iterrows():
        col_id = ref_child_column.replace("_name_cleaned", "_id")
        ref_selection = (
            df_reference[df_reference[ref_parent_column] == row_parent.ref_parent][[ref_child_column, col_id]]
            .dropna()
            .drop_duplicates()
        )
        ref_names = ref_selection[ref_child_column]

        target_names = (
            df_target_copy[df_target_copy[target_parent_column] == row_parent.target_parent][target_child_column]
            .dropna()
            .unique()
            .tolist()
        )

        print(
            f"Parent level ({target_parent_column}): {row_parent.target_parent} nr of child units in EWARS : {len(target_names)}"
        )

        # Find the best match for each name in target from reference names
        matches_list = [process.extractOne(name, ref_names, scorer=fuzz.ratio) for name in target_names]

        # build table
        df_matches = pd.DataFrame(zip(target_names, matches_list), columns=["target_child", "match_info"])
        df_matches[["ref_child", "score", "index"]] = pd.DataFrame(
            df_matches.match_info.tolist(), index=df_matches.index
        )
        df_matches = df_matches.drop(columns=["match_info", "index"])
        df_matches = df_matches.merge(ref_selection, how="left", left_on="ref_child", right_on=ref_child_column)

        for i, row_child in df_matches.iterrows():
            if row_child.score > threshold:
                parent_mask = df_target_copy[target_parent_column] == row_parent.target_parent  # level_selection_column
                df_target_copy.loc[
                    (parent_mask) & (df_target_copy[target_child_column] == row_child.target_child),
                    [ref_child_mapping_name, ref_child_mapping_id],
                ] = (row_child.ref_child, row_child[col_id])
                # add to summary
                names_matched.append(
                    [
                        row_parent.target_parent,
                        row_child.target_child,
                        row_child.ref_child,
                        row_child[col_id],
                        f"{row_child.score:.2f}%",
                    ]
                )
            else:
                names_not_matched.append(
                    [row_parent.target_parent, row_child.target_child, row_child.ref_child, f"{row_child.score:.2f}%"]
                )
    matching_summary = pd.DataFrame(
        names_matched, columns=[target_parent_column, target_child_column, "ref_match", "ref_match_id", "score"]
    )
    missing_summary = pd.DataFrame(
        names_not_matched, columns=[target_parent_column, target_child_column, "ref_closest_match", "score"]
    )

    return df_target_copy, matching_summary, missing_summary


if __name__ == "__main__":
    dhis2_ewars_push()
