@fixture.tempfolder
Feature: Command Line Interface

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
      name: generic_table
      type: source
      columns:
        "some_bools": {"type": "char(1)", "nullable": False}
        "some_ints": {"type": "int", "nullable": False}
        "some_floats": {"type": "float", "nullable": False}
        "some_dates": {"type": "date", "nullable": False}
        "some_times": {"type": "time", "nullable": False}
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "name": {"type": "varchar(max)", "nullable": False}
      """

  @fixture.jetavator
  Scenario: Load a CSV file using a Python method

    Given we call the method .deploy()

     When we call the method .load_csv("features/data/generic_table.csv", "generic_table")

     Then the table source_generic_table contains 10 rows
