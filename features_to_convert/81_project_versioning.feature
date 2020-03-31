@fixture.tempfolder
@fixture.yaml
Feature: Project Versioning

  As a Data Engineer

  I need to be able to define the project's name and version number in a file, project.yaml and be able to:
    * see the name in a deployed project with
      Client.project.name (Python) or jetavator show project name (CLI)
    * see the version in a deployed project with
      Client.project.version (Python) or jetavator show project version (CLI)
    * see the history of all previous deployments with
      Client.project_history (Python) or jetavator show project history (CLI)

  So that I can track the changes in a project over time with version numbers that I can use in
  operations and support.

  @setup
  Scenario: Project Versioning - Setup

    Given a definition for a source:
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
        source: airport_pair_details
      """

  @fixture.jetavator
  Scenario: Project Versioning - fail if no project.yaml - Python

    When we try to call the method .deploy()

    Then an exception is raised with text "Deployment failed due to missing project.yaml"

  @cli
  @fixture.jetavator
  Scenario: Project Versioning - fail if no project.yaml - CLI

    Given we run the CLI command:
      """
      jetavator config {config_args}
      """

    When we try to run the CLI command:
      """
      jetavator deploy -d
      """

    Then the command returns an error with text "Deployment failed due to missing project.yaml"

  @fixture.jetavator
  Scenario: Project Versioning - store and return name and version - Python

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

     When we call the method .deploy()

     Then the property .project.name returns "example"
      And the property .project.version returns "0.0.1"
      And the expression list({jetavator}.project_history.keys()) returns ["0.0.1"]
      And the property .project_history["0.0.1"].name returns "example"
      And the property .project_history["0.0.1"].version returns "0.0.1"
      And the property .project_history["0.0.1"].latest_version returns True
      And the property .project_history["0.0.1"].deployed_time is in the last 5 minutes

  @cli
  @fixture.jetavator
  Scenario: Project Versioning - store and return name and version - CLI

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And we run the CLI command:
      """
      jetavator config {config_args}
      """

     When we run the CLI command:
      """
      jetavator deploy -d
      """

     Then the CLI command "jetavator show project name" returns "example"
      And the CLI command "jetavator show project version" returns "0.0.1"
      And the CLI command "jetavator show project history" returns this table:
        | name    | version | deployed_time | latest_version |
        | example | 0.0.1   | *             | True           |

  @fixture.jetavator
  Scenario: Project Versioning - fail if conflicting projects

    Given a definition for a project:
      """
      name: conflicting_example
      type: project
      version: 0.0.2
      """

    When we try to call the method .deploy()

    Then an exception is raised with text "Cannot have multiple YAML definitions of type 'project'"

#//TODO:  fix test case below!
#  @cli
#  @fixture.jetavator
#  Scenario: Project Versioning - fail if conflicting projects
#
#    Given a definition for a project:
#      """
#      name: conflicting_example
#      type: project
#      version: 0.0.2
#      """
#
#    When we try to run the CLI command:
#      """
#      jetavator deploy -d
#      """
#
#    #Then the command returns an error with text "Cannot have multiple YAML definitions of type 'project'"
#    Then an exception is raised with text "Cannot have multiple YAML definitions of type 'project'"
