@fixture.tempfolder
@fixture.yaml
Feature: Link to additional hubs from hub satellites

  Scenario: Link to additional hubs from hub satellites

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
  Scenario: Initial load

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And the test data is loaded to table source_airport_pair_details
     And we call the method .run(load_type="delta")

    Then the table star.fact_airport_pair exists
     And the table star.fact_airport_pair contains 7 rows
     And the table star.fact_airport_pair contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | route_category  | num_changes |
       | ATL                 | LHR                 | Trans-Atlantic  | 0           |
       | ATL                 | ORD                 | US Domestic     | 0           |
       | ATL                 | PVG                 | Trans-Pacific   | 0           |
       | LHR                 | AMS                 | Europe          | 0           |
       | ORD                 | PVG                 | Trans-Pacific   | 1           |
       | ORD                 | LHR                 | Trans-Atlantic  | 1           |
       | ORD                 | AMS                 | Trans-Atlantic  | 2           |

     Then the table star.dim_route_category exists
      And the table star.dim_route_category contains 4 rows
      And the table star.dim_route_category contains these columns with this data:
        | hub_route_category_key |
        | Trans-Atlantic         |
        | US Domestic            |
        | Trans-Pacific          |
        | Europe                 |
