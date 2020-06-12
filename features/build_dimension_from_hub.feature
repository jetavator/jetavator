@fixture.tempfolder
@fixture.yaml
Feature: Build dimension from hub

  @setup
  Scenario: Build dimension from hub - Setup

    Given a random string in the environment variable $RANDOM_TEST_SCHEMA

    And a file saved as config.yml:
      """
      services:
        spark:
          type: local_spark
      storage:
        source: spark
        vault: spark
        star: spark
        logs: azure_queue
      compute: spark
      secret_lookup: environment
      schema: $RANDOM_TEST_SCHEMA
      """

    And a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "name": {"type": "varchar(64)", "nullable": False}
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_length: 3
      """

    And a definition for a satellite:
      """
      name: airport_details
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "name": {"type": "varchar(64)", "nullable": False}
      pipeline:
        type: source
        source: airport_details
        key_columns:
          "airport": "code"
      """

    And a definition for a satellite:
      """
      name: airport_details_allcaps
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "allcaps_name": {"type": "varchar(64)", "nullable": True}
      pipeline:
        type: sql
        key_columns:
          "airport": "hub_airport_key"
        dependencies:
          - type: satellite
            name: airport_details
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT

            hub_airport_key,
            UPPER(name) AS allcaps_name,
            sat_load_dt,
            sat_deleted_ind

          FROM {{satellite.airport_details.updates}} AS airport_details
      """

    And a definition for a satellite:
      """
      name: last_updated_airport
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "last_updated_airport": {"type": "char(1)"}
      pipeline:
        type: sql
        key_columns:
          "airport": "hub_airport_key"
        dependencies:
          - type: satellite
            name: airport_details
            view: current
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT COALESCE(updates.hub_airport_key,
                          now.hub_airport_key) AS hub_airport_key,
                 CASE WHEN updates.hub_airport_key IS NULL
                      THEN 0
                      ELSE 1
                    END AS last_updated_airport,
                 CURRENT_TIMESTAMP AS sat_load_dt,
                 COALESCE(updates.sat_deleted_ind,
                          now.sat_deleted_ind) AS sat_deleted_ind
            FROM {{satellite.airport_details.current}} AS now
                 FULL JOIN {{satellite.airport_details.updates}} AS updates
                        ON updates.hub_airport_key
                         = now.hub_airport_key
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

    And we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """

  Scenario: Initial load

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details.csv"
       """

    Then the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 10 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
      | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
      | ATL             | Hartsfield-Jackson Atlanta International Airport | HARTSFIELD-JACKSON ATLANTA INTERNATIONAL AIRPORT | 1                    |
      | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 1                    |
      | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 1                    |
      | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 1                    |
      | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 1                    |
      | ORD             | Orchard Field                                    | ORCHARD FIELD                                    | 1                    |
      | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 1                    |
      | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 1                    |
      | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 1                    |
      | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 1                    |


  Scenario: Delta load - Update

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details.csv"
       """
     And the following CSV file airport_details_update.csv is saved in the temporary folder:
        | code | name                                             |
        | ORD  | O'Hare International Airport                     |
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details_update.csv"
       """

    Then the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 10 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | Hartsfield-Jackson Atlanta International Airport | HARTSFIELD-JACKSON ATLANTA INTERNATIONAL AIRPORT | 0                    |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 0                    |
       | ORD             | O'Hare International Airport                     | O'HARE INTERNATIONAL AIRPORT                     | 1                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |

  Scenario: Delta load - Insert

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details.csv"
       """
     And the following CSV file airport_details_insert.csv is saved in the temporary folder:
        | code | name                                             |
        | AMS  | Amsterdam Airport Schiphol                       |
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details_insert.csv"
       """

    Then the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 11 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | Hartsfield-Jackson Atlanta International Airport | HARTSFIELD-JACKSON ATLANTA INTERNATIONAL AIRPORT | 0                    |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 0                    |
       | ORD             | Orchard Field                                    | ORCHARD FIELD                                    | 0                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |
       | AMS             | Amsterdam Airport Schiphol                       | AMSTERDAM AIRPORT SCHIPHOL                       | 1                    |

   Scenario: Delta load - Delete

   When all the definitions are saved to disk
    And we run the CLI command "jetavator deploy -d"
    And we run the CLI command:
      """
      jetavator run delta \
        --csv airport_details="{tempfolder}/airport_details.csv"
      """
    And the following CSV file airport_details_delete.csv is saved in the temporary folder:
       | code | jetavator_deleted_ind                            |
       | ATL  | 1                                                |
    And we run the CLI command:
      """
      jetavator run delta \
        --csv airport_details="{tempfolder}/airport_details_delete.csv"
      """

     Then the table star_dim_airport exists on the star datastore
      And the table star_dim_airport on the star datastore contains 9 rows
      And the table star_dim_airport on the star datastore contains these columns with this data:
        | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
        | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
        | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
        | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
        | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 0                    |
        | ORD             | Orchard Field                                    | ORCHARD FIELD                                    | 0                    |
        | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
        | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
        | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
        | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |

    Scenario: Allow users to exclude hubs from the star schema

      Given a definition for a hub:
        """
        name: airport
        type: hub
        key_length: 3
        exclude_from_star_schema: True
        """

      When all the definitions are saved to disk
       And we run the CLI command "jetavator deploy -d"

      Then the table star_dim_airport does not exist on the star datastore

    Scenario: Allow users to exclude hub satellites from the star schema

      Given a definition for a satellite:
        """
        name: airport_details
        type: satellite
        parent:
          name: airport
          type: hub
        columns:
          "name": {"type": "varchar(64)", "nullable": False}
        pipeline:
          type: source
          source: airport_details
          key_columns:
            "airport": "code"
        exclude_from_star_schema: True
        """

      And a definition for a hub:
        """
        name: airport
        type: hub
        key_length: 3
        """

      When all the definitions are saved to disk
       And we run the CLI command "jetavator deploy -d"

      Then the column name does not exist in the table star_dim_airport on the star datastore
