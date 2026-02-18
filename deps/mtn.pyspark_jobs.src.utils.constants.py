id_col = "Msisdn"

agg_type_columns = {
    "brand": "entity_id",
    "topic": "topic_id"
}


def get_agg_col(agg_type):
    try:
        return agg_type_columns[agg_type]
    except KeyError:
        raise ValueError(f"Invalid aggregation type '{agg_type}. Valid values are {','.join(agg_type_columns.keys())}")