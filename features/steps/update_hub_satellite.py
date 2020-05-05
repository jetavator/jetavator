from behave import given, when, then


@then(
    u"the loading datetime in the table sat_airport_details "
    "contains {row_count} distinct datetimes with code {code}"
)
def step_impl(context, row_count, code):
    assert int(context.jetavator.sql_query_single_value(
        f"""
        SELECT COUNT(DISTINCT [sat_load_dt])
        FROM [vault].[sat_airport_details]
        WHERE [hub_airport_key] = '{code}'
        """
    )) == int(row_count), (
        f"Row count does not match expected value {row_count}"
    )
