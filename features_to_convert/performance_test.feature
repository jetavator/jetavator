@fixture.tempfolder
@fixture.yaml
Feature: Performance Test

  @setup
  Scenario: Performance Test - Setup

  Given a definition for a project:
    """
    name: example
    type: project
    version: 0.0.1
    """

  And a definition for a source:
    """
    name: airport_pair_details
    type: source
    columns:
      "dep_airport": {"type": "char(3)", "nullable": False, "pk": True}
      "arr_airport": {"type": "char(3)", "nullable": False, "pk": True}
      "route_category": {"type": "varchar(100)", "nullable": False}
      "num_changes": {"type": "int", "nullable": False}
    """

  And a definition for a hub:
    """
    name: airport
    type: hub
    key_length: 3
    """

  And a definition for a hub:
    """
    name: route_category
    type: hub
    key_length: 100
    """

  And a definition for a link:
    """
    name: airport_pair
    type: link
    link_hubs:
      dep_airport: airport
      arr_airport: airport
    """

  And a definition for a satellite:
    """
    name: airport_pair_details
    type: satellite
    parent:
      name: airport_pair
      type: link
    columns:
      "route_category": {"type": "varchar(100)", "nullable": False, hub_reference: "route_category"}
      "num_changes": {"type": "int", "nullable": False}
    pipeline:
      type: source
    """

  And a definition for a satellite:
    """
    name: airport_pair_direct_flight
    type: satellite
    parent:
      name: airport_pair
      type: link
    columns:
      "direct_flight": {"type": "char(1)", "nullable": True}
    pipeline:
      type: sql
      key_columns:
        "dep_airport": "hub_dep_airport_key"
        "arr_airport": "hub_arr_airport_key"
      dependencies:
        - type: satellite
          name: airport_pair_details
      load_dt: "sat_load_dt"
      deleted_ind: "sat_deleted_ind"
      performance_hints:
        no_compress_versions_in_query: True
        no_compress_versions_in_history: True
        no_update_hubs: True
        no_update_links: True
      sql: |
        SELECT

          hub_dep_airport_key,
          hub_arr_airport_key,
          CASE
            WHEN num_changes = 0 THEN 1
            WHEN num_changes > 0 THEN 0
          END AS direct_flight,
          sat_load_dt,
          sat_deleted_ind

        FROM {{satellite.airport_pair_details.updates}} AS airport_pair_details
    """

  And a set of test data:
    | dep_airport | arr_airport | route_category  | num_changes |
    | ATL         | LHR         | Trans-Atlantic  | 0           |
    | ATL         | ORD         | US Domestic     | 0           |
    | ATL         | PVG         | Trans-Pacific   | 0           |
    | LHR         | AMS         | Europe          | 0           |
    | ORD         | PVG         | Trans-Pacific   | 1           |
    | ORD         | LHR         | Trans-Atlantic  | 1           |
    | ORD         | AMS         | Trans-Atlantic  | 2           |

  @fixture.jetavator
  Scenario Outline: Get Performance Data after Load

     When we call the method .deploy()
      And the test data is loaded to table source_airport_pair_details
      And we call the method .run(load_type="<type>")

     Then the method .get_performance_data() returns this table:
        | type      | name                       | stage                        | start_timestamp | end_timestamp | duration | rows |
        | satellite | airport_pair_details       | run_pipeline_query           | *               | *             | *        | *    |
        | satellite | airport_pair_details       | compress_versions_in_query   | *               | *             | *        | *    |
        | satellite | airport_pair_details       | compress_versions_in_history | *               | *             | *        | *    |
        | satellite | airport_pair_details       | update_hubs                  | *               | *             | *        | *    |
        | satellite | airport_pair_details       | update_links                 | *               | *             | *        | *    |
        | satellite | airport_pair_details       | update_referenced_hubs       | *               | *             | *        | *    |
        | satellite | airport_pair_details       | load_satellite               | *               | *             | *        | *    |
        | satellite | airport_pair_details       | update_pit_table             | *               | *             | *        | *    |
        | satellite | airport_pair_direct_flight | run_pipeline_query           | *               | *             | *        | *    |
        | satellite | airport_pair_direct_flight | load_satellite               | *               | *             | *        | *    |
        | satellite | airport_pair_direct_flight | update_pit_table             | *               | *             | *        | *    |

      Examples: Load Types
        | type  | hub_rows | link_rows |
        | delta | 5        | 6         |
        | full  | 5        | 6         |
