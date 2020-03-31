@fixture.tempfolder
@fixture.yaml
Feature: Multiload CSV

  @setup
  Scenario: Multiload CSV - Setup

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a source:
      """
      name: airport_details_original
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "original_name": {"type": "varchar(64)", "nullable": False}
      """

    And a definition for a source:
      """
      name: airport_details_current
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "current_name": {"type": "varchar(64)", "nullable": False}
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_length: 3
      """

    And a definition for a satellite:
      """
      name: airport_details_original
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "original_name": {"type": "varchar(64)", "nullable": False}
      pipeline:
        type: source
        source: airport_details_original
        key_columns:
          "airport": "code"
      """

    And a definition for a satellite:
      """
      name: airport_details_current
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "current_name": {"type": "varchar(64)", "nullable": False}
      pipeline:
        type: source
        source: airport_details_current
        key_columns:
          "airport": "code"
      """

    And we run the CLI command:
      """
      jetavator config {config_args}
      """

    And a CSV file airport_details_original.csv saved in a temporary folder:
      | hub_airport_key | original_name                                    |
      | ORD             | Orchard Field                                    |

    And a CSV file airport_details_current_1.csv saved in a temporary folder:
      | hub_airport_key | current_name                                     |
      | ORD             | O'Hare International Airport                     |

    And a CSV file airport_details_current_2.csv saved in a temporary folder:
      | hub_airport_key | current_name                                     |
      | ATL             | Hartsfield-Jackson Atlanta International Airport |

  @fixture.jetavator
  Scenario: Multiload CSV

    When all the definitions are saved to disk
    And we run the CLI command "jetavator deploy"

    And we run the CLI command:
      """
      jetavator run delta \
        --csv "airport_details_original={tempfolder}/airport_details_original.csv" \
        --csv "airport_details_current={tempfolder}/airport_details_current_1.csv"\
        --csv "airport_details_current={tempfolder}/airport_details_current_2.csv"
      """

    Then the table vault_sat_airport_details_original exists on the vault datastore
    And the table vault_sat_airport_details_original on the vault datastore contains 1 rows

    Then the table vault_sat_airport_details_current exists on the vault datastore
    And the table vault_sat_airport_details_current on the vault datastore contains 2 rows
