@fixture.tempfolder
@fixture.yaml
Feature: Define Link Satellite

  @setup
  Scenario: Define Link Satellite - Setup

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
        "direct_flight": {"type": "char(1)", "nullable": False}
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_length: 3
      """

    And a definition for a link:
      """
      name: airport_pair
      type: link
      link_hubs:
        dep_airport: airport
        arr_airport: airport
      """

    And a set of test data:
      | dep_airport | arr_airport | direct_flight |
      | ATL         | LHR         | 1             |
      | ATL         | DXB         | 1             |
      | ATL         | ORD         | 1             |
      | ATL         | PVG         | 1             |
      | ORD         | PVG         | 0             |
      | ORD         | LHR         | 0             |

  @fixture.jetavator
  Scenario Outline: Define Link Satellite from Source Table

    Given a definition for a satellite:
      """
      name: airport_pair_details
      type: satellite
      parent:
        name: airport_pair
        type: link
      columns:
        "direct_flight": {"type": "char(1)", "nullable": False}
      pipeline:
        type: source
      """

     When we call the method .deploy()
      And the test data is loaded to table source_airport_pair_details
      And we call the method .run(load_type="<type>")

     Then the table vault.hub_airport exists
      And the table vault.link_airport_pair exists
      And the table vault.sat_airport_pair_details exists
      And the table vault.hub_airport contains <hub_rows> rows
      And the table vault.link_airport_pair contains <link_rows> rows
      And the table vault.sat_airport_pair_details contains <link_rows> rows

      Examples: Load Types
        | type  | hub_rows | link_rows |
        | delta | 5        | 6         |
        | full  | 5        | 6         |
