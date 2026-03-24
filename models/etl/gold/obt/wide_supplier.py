def create_dataset(fct_shipping, dim_supplier):
    return (
        fct_shipping.join(
            dim_supplier, on="suppkey", how="left", suffix="_supplier"
        )
        .rename(
            {
                "comment_supplier": "supplier_comment",
                "name_supplier" : "supplier_name"
            }
        )
    )