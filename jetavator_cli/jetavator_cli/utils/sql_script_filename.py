def sql_script_filename(ddl_element):
    """
    sqlalchemy_ddl_element: sqlalchemy.sql.ddl.DDLElement
    """
    ddl_statement_type = type(ddl_element).__visit_name__

    name = getattr(
        ddl_element.element, "name", str(ddl_element.element))
    schema = getattr(
        ddl_element.element, "schema", None)

    if schema:
        qualified_name = f"{schema}.{name}"
    else:
        qualified_name = name

    return f"{ddl_statement_type}/{qualified_name}"
