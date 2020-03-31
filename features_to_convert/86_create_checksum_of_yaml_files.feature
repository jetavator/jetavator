@fixture.tempfolder
@fixture.yaml
Feature: Create checksum of YAML files when deploying or upgrading database

  As a Data Engineer

  I need to be able to:

    * see a checksum derived from the deployed YAML files with
      jetavator show project checksum

    * compare the project checksum with the checksum of the new YAML files
      when doing jetavator upgrade, and warn unless:
        * the checksum has changed
        * the version number has not increased

    * be able to override the above warning with jetavator upgrade --force

  So that I can identify occasions where a developer has forgotten to increment
  a version number, and prompt them to do so

  @setup
  Scenario: Project Versioning - Setup

    Given a definition for a project named "0.0.1":
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a project named "0.0.2":
      """
      name: example
      type: project
      version: 0.0.2
      """

    And a definition for a source named "airport_pair_details":
      """
      name: airport_pair_details
      type: source
      columns:
        "dep_airport": {"type": "char(3)", "nullable": False, "pk": True}
        "arr_airport": {"type": "char(3)", "nullable": False, "pk": True}
        "route_category": {"type": "varchar(100)", "nullable": False}
        "num_changes": {"type": "int", "nullable": False}
      """

    And a definition for a hub named "airport":
      """
      name: airport
      type: hub
      key_length: 3
      """

    And a definition for a hub named "route_category":
      """
      name: route_category
      type: hub
      key_length: 100
      """

    And a definition for a link named "airport_pair":
      """
      name: airport_pair
      type: link
      link_hubs:
        dep_airport: airport
        arr_airport: airport
      """

    And a definition for a satellite named "airport_pair_details":
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

    And a definition for a satellite named "airport_pair_direct_flight":
      """
      name: airport_pair_direct_flight
      type: satellite
      parent:
        name: airport_pair
        type: link
      columns:
        "direct_flight": {"type": "char(1)", "nullable": True}
      pipeline:
        type: sql
        key_columns:
          "dep_airport": "hub_dep_airport_key"
          "arr_airport": "hub_arr_airport_key"
        dependencies:
          - type: satellite
            name: airport_pair_details
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT

            hub_dep_airport_key,
            hub_arr_airport_key,
            CASE
              WHEN num_changes = 0 THEN 1
              WHEN num_changes > 0 THEN 0
            END AS direct_flight,
            sat_load_dt,
            sat_deleted_ind

          FROM {{satellite.airport_pair_details.updates}} AS airport_pair_details
      """

  @fixture.jetavator
  Scenario: Create checksum of YAML files - check that checksum exists

   Given a folder "0.0.1" containing the files:
      """
      project_0.0.1
      source_airport_pair_details
      hub_airport
      hub_route_category
      link_airport_pair
      satellite_airport_pair_details
      """

    When we call the method .deploy(model_dir="{tempfolder}/0.0.1")

    Then the property .project.checksum is not None
     And the property .project_history["0.0.1"].checksum is not None

  @fixture.jetavator
  Scenario: Create checksum of YAML files - fail if checksum has not changed

   Given a folder "0.0.1" containing the files:
      """
      project_0.0.1
      source_airport_pair_details
      hub_airport
      hub_route_category
      link_airport_pair
      satellite_airport_pair_details
      """

     And a folder "0.0.2" containing the files:
      """
      project_0.0.2
      source_airport_pair_details
      hub_airport
      hub_route_category
      link_airport_pair
      satellite_airport_pair_details
      """

    When we call the method .deploy(model_dir="{tempfolder}/0.0.1")
     And we try to call the method .update(model_dir="{tempfolder}/0.0.2")

    Then an exception is raised with text "Cannot upgrade a project if the definitions have not changed"

   @fixture.jetavator
   Scenario: Create checksum of YAML files - fail if version is not incremented

    Given a folder "0.0.1" containing the files:
       """
       project_0.0.1
       source_airport_pair_details
       hub_airport
       hub_route_category
       link_airport_pair
       satellite_airport_pair_details
       """

      And a folder "0.0.2" containing the files:
       """
       project_0.0.1
       source_airport_pair_details
       hub_airport
       hub_route_category
       link_airport_pair
       satellite_airport_pair_details
       satellite_airport_pair_direct_flight
       """

     When we call the method .deploy(model_dir="{tempfolder}/0.0.1")
      And we try to call the method .update(model_dir="{tempfolder}/0.0.2")

     Then an exception is raised with text "Cannot upgrade - version number must be incremented from 0.0.1"

    @fixture.jetavator
    Scenario: Create checksum of YAML files - succeed if upgrade is valid

     Given a folder "0.0.1" containing the files:
        """
        project_0.0.1
        source_airport_pair_details
        hub_airport
        hub_route_category
        link_airport_pair
        satellite_airport_pair_details
        """

     Given a folder "0.0.2" containing the files:
        """
        project_0.0.2
        source_airport_pair_details
        hub_airport
        hub_route_category
        link_airport_pair
        satellite_airport_pair_details
        satellite_airport_pair_direct_flight
        """

      When we call the method .deploy(model_dir="{tempfolder}/0.0.1")
       And we call the method .update(model_dir="{tempfolder}/0.0.2")

      Then the expression {jetavator}.project.checksum == {jetavator}.project_history"0.0.1".checksum returns False
