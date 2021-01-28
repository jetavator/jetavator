@fixture.tempfolder
@fixture.yaml
Feature: Config CLI commands

  @setup
  Scenario: Config CLI commands - Setup

    Given a random string in the environment variable $RANDOM_TEST_SCHEMA

    And a config file saved as config.yml

    And a definition for a project:
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
        "num_changes": {"type": "int", "nullable": False}
      """

  Scenario: Set Config twice without error

    When we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """

    And we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """
