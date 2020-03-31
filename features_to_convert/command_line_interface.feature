@fixture.tempfolder
Feature: Command Line Interface

  @cli
  @setup
  @fixture.yaml
  Scenario: Command Line Interface - Setup

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
      """

    And a set of test data:
      | dep_airport | arr_airport | direct_flight |
      | ATL         | LHR         | 1             |
      | ATL         | DXB         | 1             |
      | ATL         | ORD         | 1             |
      | ATL         | PVG         | 1             |
      | ORD         | PVG         | 0             |
      | ORD         | LHR         | 0             |

    And a Behave feature file:
      """
      Feature: Example

        Scenario: Example

          Given the following data is loaded to table source_airport_pair_details:
             | dep_airport | arr_airport | direct_flight |
             | ATL         | LHR         | 1             |
             | ATL         | DXB         | 1             |
             | ATL         | ORD         | 1             |
             | ATL         | PVG         | 1             |
             | ORD         | PVG         | 0             |
             | ORD         | LHR         | 0             |

          When we execute the stored procedure vault.load_delta

          Then the table vault.sat_airport_pair_details contains 6 rows
      """

    And a csv file airport_pair_details.csv saved in a temporary folder:
      """
      dep_airport,arr_airport,direct_flight
      ATL,LHR,1
      ATL,DXB,1
      ATL,ORD,1
      ATL,PVG,1
      ORD,PVG,0
      ORD,LHR,0
      """

    And a Behave feature implementation:
      """
      import jetavator_cli.testing.behave.steps
      """

  @cli
  Scenario: Deploy to local SQL Server instance

    When we run the CLI command:
      """
      jetavator deploy -d --model-dir="{tempfolder}" --server="{server}" --db="{database_name}" --username="{username}" --password="{password}" {trusted_connection}
      """

  @cli
  Scenario: Test on local SQL Server instance

    When we run the CLI command:
      """
      jetavator test -d --model-dir="{tempfolder}" --server="{server}" --db="{database_name}" --username="{username}" --password="{password}" {trusted_connection}
      """

    Then the command returns error code 0

  @cli
  Scenario: Test Load of CSV

    When we run the CLI command:
      """
      jetavator deploy -d --model-dir="{tempfolder}" --server="{server}" --db="{database_name}"  --username="{username}" --password="{password}" {trusted_connection}
      """

    And we run the CLI command:
      """
      jetavator load csv --model-dir="{tempfolder}" --server="{server}" --db="{database_name}"  --username="{username}" --password="{password}" {trusted_connection} --file="{tempfolder}/airport_pair_details.csv" --table="airport_pair_details"
      """

    Then the command returns error code 0
     And the table source_airport_pair_details contains 6 rows

  @cli
  Scenario: Test Load of CSV with separate login

    When we run the CLI command:
      """
      jetavator login --server="{server}" --username="{username}" --password="{password}" {trusted_connection}
      """

    When we run the CLI command:
      """
      jetavator deploy -d --db="{database_name}" --model-dir="{tempfolder}"
      """

    And we run the CLI command:
      """
      jetavator load csv --file="{tempfolder}/airport_pair_details.csv" --table="airport_pair_details"
      """

    Then the command returns error code 0
    And the table source_airport_pair_details contains 6 rows

    @cli
    Scenario: Test Load of CSV with folder and get performance data

      When we run the CLI command:
        """
        jetavator login --server="{server}" --username="{username}" --password="{password}" {trusted_connection}
        """

      When we run the CLI command:
        """
        jetavator deploy -d --db="{database_name}" --model-dir="{tempfolder}"
        """

      And we run the CLI command:
        """
        jetavator load csv --folder="{tempfolder}"
        """

      And we run the CLI command:
        """
        jetavator run delta
        """

      And we run the CLI command:
        """
        jetavator performance
        """

      And we run the CLI command:
        """
        jetavator performance --outfile=artifacts/performance.csv --no-print
        """

      And we run the CLI command:
        """
        jetavator performance --pivot=rows
        """

      And we run the CLI command:
        """
        jetavator performance --pivot=duration --outfile=artifacts/duration.csv
        """

      Then the command returns error code 0
      And the table source_history.airport_pair_details contains 6 rows
