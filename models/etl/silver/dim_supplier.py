def create_dataset(supplier, nation):
    return(
        supplier.join(
            nation, on="nationkey", how="left", suffix="_nation"
        )
        .rename(
            {
                "name_nation": "nation_name",
                "comment_nation": "nation_comment"
            }
        )
    )