@fixture.tempfolder
@fixture.yaml
Feature: Allow option of using business keys instead of hashing in hub and link tables

  @fixture.jetavator
  Scenario: Configure business keys or hash key - Hub - no hash key

    Given a definition for a project:
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
        """

      And a CSV file airport_details.csv saved in a temporary folder:
        | code | name                                             |
        | ATL  | Hartsfield-Jackson Atlanta International Airport |
        | PEK  | Beijing Capital International Airport            |
        | DXB  | Dubai International Airport                      |
        | HND  | Haneda Airport                                   |
        | LAX  | Los Angeles International Airport                |
        | ORD  | O'Hare International Airport                     |
        | LHR  | London Heathrow Airport                          |
        | HKG  | Hong Kong International Airport                  |
        | PVG  | Shanghai Pudong International Airport            |
        | CDG  | Paris-Charles de Gaulle Airport	                |

      And we run the CLI command:
        """
        jetavator config {config_args}
        """

      When all the definitions are saved to disk
      And we run the CLI command "jetavator deploy"
          # Calls Client.deploy(model_dir="{tempfolder}")
      And we run the CLI command:
        """
        jetavator load csv \
          --file="{tempfolder}/airport_details.csv" \
          --table="airport_details"
        """
      And we run the CLI command "jetavator run delta"
          # Calls Client.run(load_type="delta")

      Then the column hub_airport_hash does not exist in the table vault_hub_airport
       And the column hub_airport_hash does not exist in the table vault_sat_airport_details
       And the column hub_airport_hash does not exist in the table star_dim_airport
       #And the table vault_hub_airport contains 10 rows //TODO: #119
       And the table vault_sat_airport_details contains 10 rows
       And the table star_dim_airport contains 10 rows

# //TODO: #130
  #@fixture.jetavator
  #   Scenario: Configure business keys or hash key - Hub - use hash key
  #
  #     Given a definition for a project:
  #         """
  #         name: example
  #         type: project
  #         version: 0.0.1
  #         """
  #
  #       And a definition for a source:
  #         """
  #         name: airport_details
  #         type: source
  #         columns:
  #           "code": {"type": "char(3)", "nullable": False, "pk": True}
  #           "name": {"type": "varchar(64)", "nullable": False}
  #         """
  #
  #       And a definition for a hub:
  #         """
  #         name: airport
  #         type: hub
  #         key_length: 3
  #         options:
  #           - hash_key
  #           - no_primary_key
  #         """
  #
  #       And a definition for a satellite:
  #         """
  #         name: airport_details
  #         type: satellite
  #         parent:
  #           name: airport
  #           type: hub
  #         columns:
  #           "name": {"type": "varchar(64)", "nullable": False}
  #         pipeline:
  #           type: source
  #           source: airport_details
  #         """
  #
  #       And a CSV file airport_details.csv saved in a temporary folder:
  #         | code | name                                             |
  #         | ATL  | Hartsfield-Jackson Atlanta International Airport |
  #         | PEK  | Beijing Capital International Airport            |
  #         | DXB  | Dubai International Airport                      |
  #         | HND  | Haneda Airport                                   |
  #         | LAX  | Los Angeles International Airport                |
  #         | ORD  | O'Hare International Airport                     |
  #         | LHR  | London Heathrow Airport                          |
  #         | HKG  | Hong Kong International Airport                  |
  #         | PVG  | Shanghai Pudong International Airport            |
  #         | CDG  | Paris-Charles de Gaulle Airport	              |
  #
  #        And we run the CLI command:
  #         """
  #         jetavator config {config_args}
  #         """
  #
  #       When all the definitions are saved to disk
  #       And we run the CLI command "jetavator deploy"
  #           # Calls Client.deploy(model_dir="{tempfolder}")
  #       And we run the CLI command:
  #         """
  #         jetavator load csv \
  #           --file="{tempfolder}/airport_details.csv" \
  #           --table="airport_details"
  #         """
  #       And we run the CLI command "jetavator run delta"
  #           # Calls Client.run(load_type="delta")
  #
  #      Then the column hub_airport_hash exists in the table vault_hub_airport
  #       And the column hub_airport_hash exists in the table vault_sat_airport_details
  #       And the column hub_airport_hash exists in the table star_dim_airport
  #       And the table vault_hub_airport contains 10 rows
  #       And the table vault_sat_airport_details contains 10 rows
  #       And the table star_dim_airport contains 10 rows
  @fixture.jetavator
  Scenario: Configure business keys or hash key - Link - no hash key

    Given a definition for a source:
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
        options:
          - no_primary_key
        """

      And a definition for a link:
        """
        name: airport_pair
        type: link
        link_hubs:
          dep_airport: airport
          arr_airport: airport
        options:
          - no_primary_key
        """

      And a definition for a satellite:
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
          source: airport_pair_details
        """

      And a CSV file airport_pair_details.csv saved in a temporary folder:
        | dep_airport | arr_airport | direct_flight |
        | ATL         | LHR         | 1             |
        | ATL         | DXB         | 1             |
        | ATL         | ORD         | 1             |
        | ATL         | PVG         | 1             |
        | ORD         | PVG         | 0             |
        | ORD         | LHR         | 0             |

      And we run the CLI command:
        """
        jetavator config {config_args}
        """

      When all the definitions are saved to disk
      And we run the CLI command "jetavator deploy"
          # Calls Client.deploy(model_dir="{tempfolder}")
      And we run the CLI command:
        """
        jetavator load csv \
          --file="{tempfolder}/airport_pair_details.csv" \
          --table="airport_pair_details"
        """
      And we run the CLI command "jetavator run delta"
          # Calls Client.run(load_type="delta")

     Then the column link_airport_pair_hash does not exist in the table vault_link_airport_pair
      And the column link_airport_pair_hash does not exist in the table vault_sat_airport_pair_details
      And the column link_airport_pair_hash does not exist in the table star_fact_airport_pair
      #And the table vault_hub_airport contains 5 rows //TODO: #119
      #And the table vault_link_airport_pair contains 6 rows //TODO: #119
      And the table vault_sat_airport_pair_details contains 6 rows
      And the table star_fact_airport_pair contains 6 rows

# //TODO: #130
#  @fixture.jetavator
#  Scenario: Configure business keys or hash key - Link - use hash keys
#
#    Given a definition for a project:
#        """
#        name: example
#        type: project
#        version: 0.0.1
#        """
#
#      And a definition for a source:
#        """
#        name: airport_pair_details
#        type: source
#        columns:
#          "dep_airport": {"type": "char(3)", "nullable": False, "pk": True}
#          "arr_airport": {"type": "char(3)", "nullable": False, "pk": True}
#          "direct_flight": {"type": "char(1)", "nullable": False}
#        """
#
#      And a definition for a hub:
#        """
#        name: airport
#        type: hub
#        key_length: 3
#        options:
#          - hash_key
#          - no_primary_key
#        """
#
#      And a definition for a link:
#        """
#        name: airport_pair
#        type: link
#        link_hubs:
#          dep_airport: airport
#          arr_airport: airport
#        options:
#          - hash_key
#          - no_primary_key
#        """
#
#      And a definition for a satellite:
#        """
#        name: airport_pair_details
#        type: satellite
#        parent:
#          name: airport_pair
#          type: link
#        columns:
#          "direct_flight": {"type": "char(1)", "nullable": False}
#        pipeline:
#          type: source
#          source: airport_pair_details
#        """
#
#      And a CSV file airport_pair_details.csv saved in a temporary folder:
#        | dep_airport | arr_airport | direct_flight |
#        | ATL         | LHR         | 1             |
#        | ATL         | DXB         | 1             |
#        | ATL         | ORD         | 1             |
#        | ATL         | PVG         | 1             |
#        | ORD         | PVG         | 0             |
#        | ORD         | LHR         | 0             |
#
#
#      And we run the CLI command:
#        """
#        jetavator config {config_args}
#        """
#
#      When all the definitions are saved to disk
#      And we run the CLI command "jetavator deploy"
#          # Calls Client.deploy(model_dir="{tempfolder}")
#      And we run the CLI command:
#        """
#        jetavator load csv \
#          --file="{tempfolder}/airport_pair_details.csv" \
#          --table="airport_pair_details"
#        """
#      And we run the CLI command "jetavator run delta"
#          # Calls Client.run(load_type="delta")
#
#     Then the column link_airport_pair_hash exists in the table vault_link_airport_pair
#      And the column link_airport_pair_hash exists in the table vault_sat_airport_pair_details
#      And the column link_airport_pair_hash exists in the table star_fact_airport_pair
#      And the table vault_hub_airport contains 5 rows
#      And the table vault_link_airport_pair contains 6 rows
#      And the table vault_sat_airport_pair_details contains 6 rows
#      And the table star_fact_airport_pair contains 6 rows
