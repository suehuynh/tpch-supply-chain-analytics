# Dataset to store shipping days for each order
def create_dataset(lineitem, orders):
    return(
        lineitem.join(
            orders, on="orderkey", how="inner", suffix="_order"
        )
        .rename(
            {
            "comment_order" : "order_comment"
            }
        )
    )