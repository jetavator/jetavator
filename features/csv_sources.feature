@fixture.tempfolder
@fixture.yaml
Feature: CSV sources

  @setup
  Scenario: CSV sources - Setup

    Given a random string in the environment variable $RANDOM_TEST_SCHEMA

    And a config file saved as config.yml

    And a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_type: "CHAR(3)"
      """

    And a definition for a satellite:
      """
      name: airport_details
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "name": {"type": "String(64)", "nullable": False}
      pipeline:
        type: source
        source: airport_details
        key_columns:
          "airport": "code"
      """

    And we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """

  @fixture.remove_database_after_scenario
  @fixture.freeze_time
  Scenario: No extra indicators

    Given a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "name": {"type": "String(64)", "nullable": False}
      """

    And a CSV file airport_details.csv saved in a temporary folder:
      | code | name                                             |
      | ATL  | Hartsfield-Jackson Atlanta International Airport |
      | PEK  | Beijing Capital International Airport            |
      | DXB  | Dubai International Airport                      |
      | HND  | Haneda Airport                                   |
      | LAX  | Los Angeles International Airport                |
      | ORD  | Orchard Field                                    |
      | LHR  | London Heathrow Airport                          |
      | HKG  | Hong Kong International Airport                  |
      | PVG  | Shanghai Pudong International Airport            |
      | CDG  | Paris-Charles de Gaulle Airport	                |

    And the system time is 2021-01-02 12:34:56

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta --csv airport_details="{tempfolder}/airport_details.csv"
       """

    Then the table vault_sat_airport_details exists on the vault datastore
     And the table vault_sat_airport_details on the vault datastore contains 10 rows
     And the table vault_sat_airport_details on the vault datastore contains these columns with this data:
      | hub_airport_key | name                                             | sat_load_dt         | sat_deleted_ind |
      | ATL             | Hartsfield-Jackson Atlanta International Airport | 2021-01-02 12:34:56 | False           |
      | PEK             | Beijing Capital International Airport            | 2021-01-02 12:34:56 | False           |
      | DXB             | Dubai International Airport                      | 2021-01-02 12:34:56 | False           |
      | HND             | Haneda Airport                                   | 2021-01-02 12:34:56 | False           |
      | LAX             | Los Angeles International Airport                | 2021-01-02 12:34:56 | False           |
      | ORD             | Orchard Field                                    | 2021-01-02 12:34:56 | False           |
      | LHR             | London Heathrow Airport                          | 2021-01-02 12:34:56 | False           |
      | HKG             | Hong Kong International Airport                  | 2021-01-02 12:34:56 | False           |
      | PVG             | Shanghai Pudong International Airport            | 2021-01-02 12:34:56 | False           |
      | CDG             | Paris-Charles de Gaulle Airport                  | 2021-01-02 12:34:56 | False           |


  @fixture.remove_database_after_scenario
  @fixture.freeze_time
  Scenario: With deleted indicator

    Given a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "name": {"type": "String(64)", "nullable": False}
        "my_deleted_ind": {"type": "Integer", "nullable": False}
      deleted_indicator_column: my_deleted_ind
      """

    And a CSV file airport_details.csv saved in a temporary folder:
      | code | name                                             | my_deleted_ind |
      | ATL  | Hartsfield-Jackson Atlanta International Airport | 0              |
      | PEK  | Beijing Capital International Airport            | 0              |
      | DXB  | Dubai International Airport                      | 0              |
      | HND  | Haneda Airport                                   | 0              |
      | LAX  | Los Angeles International Airport                | 0              |
      | ORD  | Orchard Field                                    | 1              |
      | LHR  | London Heathrow Airport                          | 0              |
      | HKG  | Hong Kong International Airport                  | 0              |
      | PVG  | Shanghai Pudong International Airport            | 0              |
      | CDG  | Paris-Charles de Gaulle Airport	                | 0              |

    And the system time is 2021-01-02 12:34:56

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta --csv airport_details="{tempfolder}/airport_details.csv"
       """

    Then the table vault_sat_airport_details exists on the vault datastore
     And the table vault_sat_airport_details on the vault datastore contains 10 rows
     And the table vault_sat_airport_details on the vault datastore contains these columns with this data:
      | hub_airport_key | name                                             | sat_load_dt         | sat_deleted_ind |
      | ATL             | Hartsfield-Jackson Atlanta International Airport | 2021-01-02 12:34:56 | False           |
      | PEK             | Beijing Capital International Airport            | 2021-01-02 12:34:56 | False           |
      | DXB             | Dubai International Airport                      | 2021-01-02 12:34:56 | False           |
      | HND             | Haneda Airport                                   | 2021-01-02 12:34:56 | False           |
      | LAX             | Los Angeles International Airport                | 2021-01-02 12:34:56 | False           |
      | ORD             | Orchard Field                                    | 2021-01-02 12:34:56 | True            |
      | LHR             | London Heathrow Airport                          | 2021-01-02 12:34:56 | False           |
      | HKG             | Hong Kong International Airport                  | 2021-01-02 12:34:56 | False           |
      | PVG             | Shanghai Pudong International Airport            | 2021-01-02 12:34:56 | False           |
      | CDG             | Paris-Charles de Gaulle Airport                  | 2021-01-02 12:34:56 | False           |

  @fixture.remove_database_after_scenario
  Scenario: With load timestamp in custom format

    Given a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "name": {"type": "String(64)", "nullable": False}
        "my_load_dt": {"type": "DateTime", "nullable": False}
      load_timestamp_column: my_load_dt
      date_format: "dd.MM.yyyy"
      timestamp_format: "dd.MM.yyyy HH:mm:ss"
      """

    And a CSV file airport_details.csv saved in a temporary folder:
      | code | name                                             | my_load_dt          |
      | ATL  | Hartsfield-Jackson Atlanta International Airport | 2021-01-01 00:00:00 |
      | PEK  | Beijing Capital International Airport            | 2021-01-01 00:00:00 |
      | DXB  | Dubai International Airport                      | 2021-02-02 02:46:00 |
      | HND  | Haneda Airport                                   | 2021-02-02 02:46:00 |
      | LAX  | Los Angeles International Airport                | 2021-01-01 00:00:00 |
      | ORD  | Orchard Field                                    | 2021-02-02 02:46:00 |
      | LHR  | London Heathrow Airport                          | 2021-02-02 02:46:00 |
      | HKG  | Hong Kong International Airport                  | 2021-01-01 00:00:00 |
      | PVG  | Shanghai Pudong International Airport            | 2021-02-02 02:46:00 |
      | CDG  | Paris-Charles de Gaulle Airport	                | 2021-01-01 00:00:00 |

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta --csv airport_details="{tempfolder}/airport_details.csv"
       """

    Then the table vault_sat_airport_details exists on the vault datastore
     And the table vault_sat_airport_details on the vault datastore contains 10 rows
     And the table vault_sat_airport_details on the vault datastore contains these columns with this data:
      | hub_airport_key | name                                             | sat_load_dt         | sat_deleted_ind |
      | ATL             | Hartsfield-Jackson Atlanta International Airport | 2021-01-01 00:00:00 | False           |
      | PEK             | Beijing Capital International Airport            | 2021-01-01 00:00:00 | False           |
      | DXB             | Dubai International Airport                      | 2021-02-02 02:46:00 | False           |
      | HND             | Haneda Airport                                   | 2021-02-02 02:46:00 | False           |
      | LAX             | Los Angeles International Airport                | 2021-01-01 00:00:00 | False           |
      | ORD             | Orchard Field                                    | 2021-02-02 02:46:00 | False           |
      | LHR             | London Heathrow Airport                          | 2021-02-02 02:46:00 | False           |
      | HKG             | Hong Kong International Airport                  | 2021-01-01 00:00:00 | False           |
      | PVG             | Shanghai Pudong International Airport            | 2021-02-02 02:46:00 | False           |
      | CDG             | Paris-Charles de Gaulle Airport                  | 2021-01-01 00:00:00 | False           |

  @fixture.remove_database_after_scenario
  Scenario: With load timestamp

    Given a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "name": {"type": "String(64)", "nullable": False}
        "my_load_dt": {"type": "DateTime", "nullable": False}
      load_timestamp_column: my_load_dt
      """

    And a CSV file airport_details.csv saved in a temporary folder:
      | code | name                                             | my_load_dt          |
      | ATL  | Hartsfield-Jackson Atlanta International Airport | 2021-01-01 00:00:00 |
      | PEK  | Beijing Capital International Airport            | 2021-01-01 00:00:00 |
      | DXB  | Dubai International Airport                      | 2021-02-02 02:46:00 |
      | HND  | Haneda Airport                                   | 2021-02-02 02:46:00 |
      | LAX  | Los Angeles International Airport                | 2021-01-01 00:00:00 |
      | ORD  | Orchard Field                                    | 2021-02-02 02:46:00 |
      | LHR  | London Heathrow Airport                          | 2021-02-02 02:46:00 |
      | HKG  | Hong Kong International Airport                  | 2021-01-01 00:00:00 |
      | PVG  | Shanghai Pudong International Airport            | 2021-02-02 02:46:00 |
      | CDG  | Paris-Charles de Gaulle Airport	                | 2021-01-01 00:00:00 |

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta --csv airport_details="{tempfolder}/airport_details.csv"
       """

    Then the table vault_sat_airport_details exists on the vault datastore
     And the table vault_sat_airport_details on the vault datastore contains 10 rows
     And the table vault_sat_airport_details on the vault datastore contains these columns with this data:
      | hub_airport_key | name                                             | sat_load_dt         | sat_deleted_ind |
      | ATL             | Hartsfield-Jackson Atlanta International Airport | 2021-01-01 00:00:00 | False           |
      | PEK             | Beijing Capital International Airport            | 2021-01-01 00:00:00 | False           |
      | DXB             | Dubai International Airport                      | 2021-02-02 02:46:00 | False           |
      | HND             | Haneda Airport                                   | 2021-02-02 02:46:00 | False           |
      | LAX             | Los Angeles International Airport                | 2021-01-01 00:00:00 | False           |
      | ORD             | Orchard Field                                    | 2021-02-02 02:46:00 | False           |
      | LHR             | London Heathrow Airport                          | 2021-02-02 02:46:00 | False           |
      | HKG             | Hong Kong International Airport                  | 2021-01-01 00:00:00 | False           |
      | PVG             | Shanghai Pudong International Airport            | 2021-02-02 02:46:00 | False           |
      | CDG             | Paris-Charles de Gaulle Airport                  | 2021-01-01 00:00:00 | False           |

  @fixture.remove_database_after_scenario
  Scenario: With both deleted indicator and load timestamp

    Given a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "name": {"type": "String(64)", "nullable": False}
        "my_deleted_ind": {"type": "Integer", "nullable": False}
        "my_load_dt": {"type": "DateTime", "nullable": False}
      deleted_indicator_column: my_deleted_ind
      load_timestamp_column: my_load_dt
      """

    And a CSV file airport_details.csv saved in a temporary folder:
      | code | name                                             | my_deleted_ind | my_load_dt          |
      | ATL  | Hartsfield-Jackson Atlanta International Airport | 0              | 2021-01-01 00:00:00 |
      | PEK  | Beijing Capital International Airport            | 0              | 2021-01-01 00:00:00 |
      | DXB  | Dubai International Airport                      | 0              | 2021-02-02 02:46:00 |
      | HND  | Haneda Airport                                   | 0              | 2021-02-02 02:46:00 |
      | LAX  | Los Angeles International Airport                | 0              | 2021-01-01 00:00:00 |
      | ORD  | Orchard Field                                    | 0              | 2021-02-02 02:46:00 |
      | LHR  | London Heathrow Airport                          | 0              | 2021-02-02 02:46:00 |
      | HKG  | Hong Kong International Airport                  | 0              | 2021-01-01 00:00:00 |
      | PVG  | Shanghai Pudong International Airport            | 0              | 2021-02-02 02:46:00 |
      | CDG  | Paris-Charles de Gaulle Airport	                | 1              | 2021-01-01 00:00:00 |

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta --csv airport_details="{tempfolder}/airport_details.csv"
       """

    Then the table vault_sat_airport_details exists on the vault datastore
     And the table vault_sat_airport_details on the vault datastore contains 10 rows
     And the table vault_sat_airport_details on the vault datastore contains these columns with this data:
      | hub_airport_key | name                                             | sat_load_dt         | sat_deleted_ind |
      | ATL             | Hartsfield-Jackson Atlanta International Airport | 2021-01-01 00:00:00 | False           |
      | PEK             | Beijing Capital International Airport            | 2021-01-01 00:00:00 | False           |
      | DXB             | Dubai International Airport                      | 2021-02-02 02:46:00 | False           |
      | HND             | Haneda Airport                                   | 2021-02-02 02:46:00 | False           |
      | LAX             | Los Angeles International Airport                | 2021-01-01 00:00:00 | False           |
      | ORD             | Orchard Field                                    | 2021-02-02 02:46:00 | False           |
      | LHR             | London Heathrow Airport                          | 2021-02-02 02:46:00 | False           |
      | HKG             | Hong Kong International Airport                  | 2021-01-01 00:00:00 | False           |
      | PVG             | Shanghai Pudong International Airport            | 2021-02-02 02:46:00 | False           |
      | CDG             | Paris-Charles de Gaulle Airport                  | 2021-01-01 00:00:00 | True            |