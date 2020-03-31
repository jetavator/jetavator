@fixture.tempfolder
@fixture.yaml
Feature: Add Timestamps On Source

  @fixture.jetavator
  Scenario: Add Timestamps On Source

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a source:
      """
      name: example
      type: source
      columns:
        "example_key": {"type": "int", "nullable": False, "pk": True}
        "example_int": {"type": "int", "nullable": False}
        "example_optional_varchar": {"type": "varchar(50)", "nullable": True}
      """
     When we call the method .deploy()
     Then the source table will have an additional datetime column, jetavator_load_dt
      And the source table will have an additional bit column, jetavator_deleted_ind

  @fixture.jetavator
  Scenario: Populate Timestamps On Load

    Given a definition for a project:
  """
  name: example
  type: project
  version: 0.0.1
  """

And a definition for a source:
      """
      name: example
      type: source
      columns:
        "example_key": {"type": "int", "nullable": False, "pk": True}
        "example_int": {"type": "int", "nullable": False}
        "example_optional_varchar": {"type": "varchar(50)", "nullable": True}
      """
     When we call the method .deploy()
      And test data is loaded into the source table:
        | example_key | example_int | example_optional_varchar |
        | 123         | 10          |                          |
        | 124         | 20          | Lorem ipsum              |
        | 125         | 5           | Dolor sit amet           |

     Then the column jetavator_load_dt is populated with the current UTC timestamp for all rows
      And the table source_example contains 3 rows
