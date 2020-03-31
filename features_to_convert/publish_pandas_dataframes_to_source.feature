@fixture.tempfolder
@fixture.yaml
Feature: Publish pandas dataframe to source of an existing database

  As a data scientist...

  I need to save an existing pandas dataframe as a source table to the database
  which I am working on...

  So that...
    1.  I can save data that I have downloaded in Python from an external
        source, e.g. screen scraped data, without having to serialise it to CSV

    2.  I can save a pandas dataframe that I derived from existing platform
        data, but cannot easily express in SQL

  Notes: pandas dataframes have a .to_sql() method that can save a dataframe to
  an existing SQL table, using an SQLAlchemy connection (which we already have
  in the application). We would need to check that the column names and data
  types match the definition. The definition of the intended data type mapping
  is stored in PANDAS_TO_SQL_MAPPINGS in model.py.

  Test Design:
    Must-have: For each pandas data type try to load a corresponding dataframe
    and check if all rows are present on SQL side, or even better: download the
    dataframe and see if is equivalent to the original dataframe.

    Must-have: Try to reload the same dataframe without dropping it before hand.
    This should fail in the add_object step.

    Nice-to-have: Try to think of pandas dataframes that are currently not
    supported and should therefore fail e.g. multiindices.

  @setup
  Scenario: Put some initial data into the database

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a source:
      """
      name: original_airport_details
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "name": {"type": "varchar(max)", "nullable": False}
      """

    And a set of test data:
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

  @fixture.jetavator
  Scenario: Publish pandas data frame under a new name

    When we call the method .deploy()
    And the test data is loaded to table source_original_airport_details
    And we call the method .run(load_type="delta")
    And we publish below pandas dataframe as source what_data_scientist_created using code as primary key:
      | bool        | int       | float       | datetime64[ns]    | str  | object                                           |
      | some_bools  | some_ints | some_floats | some_dates        | code | name                                             |
      | False       | 1         | 1.2         | 2015-01-01        | ATL  | Hartsfield-Jackson Atlanta International Airport |
      | False       | 2         | -1.2        | 2015-01-01        | PEK  | Beijing Capital International Airport            |
      | False       | 3         | 1.71        | 2015-01-01        | DXB  | Dubai International Airport                      |
      | False       | 4         | 1.2         | 2015-01-02        | HND  | Haneda Airport                                   |
      | False       | 5         | 1.2         | 2015-01-02        | LAX  | Los Angeles International Airport                |
      | False       | 6         | 1.9         | 2015-01-02        | ORD  | O'Hare International Airport                     |
      | False       | 7         | 1.2         | 2015-01-02        | LHR  | London Heathrow Airport                          |
      | False       | 8         | 1.2         | 2015-01-02        | HKG  | Hong Kong International Airport                  |
      | False       | 9         | 1.2         | 2015-01-02        | PVG  | Shanghai Pudong International Airport            |
      | False       | 0         | 1.2         | 2015-01-03        | CDG  | Paris-Charles de Gaulle Airport	                |

    Then the table source_what_data_scientist_created contains these columns with this data:
      | some_bools  | some_ints | some_floats | some_dates        | code | name                                             |
      | False       | 1         | 1.2         | 2015-01-01        | ATL  | Hartsfield-Jackson Atlanta International Airport |
      | False       | 2         | -1.2        | 2015-01-01        | PEK  | Beijing Capital International Airport            |
      | False       | 3         | 1.71        | 2015-01-01        | DXB  | Dubai International Airport                      |
      | False       | 4         | 1.2         | 2015-01-02        | HND  | Haneda Airport                                   |
      | False       | 5         | 1.2         | 2015-01-02        | LAX  | Los Angeles International Airport                |
      | False       | 6         | 1.9         | 2015-01-02        | ORD  | O'Hare International Airport                     |
      | False       | 7         | 1.2         | 2015-01-02        | LHR  | London Heathrow Airport                          |
      | False       | 8         | 1.2         | 2015-01-02        | HKG  | Hong Kong International Airport                  |
      | False       | 9         | 1.2         | 2015-01-02        | PVG  | Shanghai Pudong International Airport            |
      | False       | 0         | 1.2         | 2015-01-03        | CDG  | Paris-Charles de Gaulle Airport	                |

  @fixture.jetavator
  Scenario: Try to publish data frame under the name of an already exsiting table

    When we call the method .deploy()
    And the test data is loaded to table source_original_airport_details
    And we call the method .run(load_type="delta")
    And we try to publish below pandas dataframe as source original_airport_details using code as primary key:
      | bool        | int       | float       | datetime64[ns]    | str  | object                                           |
      | some_bools  | some_ints | some_floats | some_dates        | code | name                                             |
      | False       | 1         | 1.2         | 2015-01-01        | ATL  | Hartsfield-Jackson Atlanta International Airport |
      | False       | 2         | -1.2        | 2015-01-01        | PEK  | Beijing Capital International Airport            |
      | False       | 3         | 1.71        | 2015-01-01        | DXB  | Dubai International Airport                      |
      | False       | 4         | 1.2         | 2015-01-02        | HND  | Haneda Airport                                   |
      | False       | 5         | 1.2         | 2015-01-02        | LAX  | Los Angeles International Airport                |
      | False       | 6         | 1.9         | 2015-01-02        | ORD  | O'Hare International Airport                     |
      | False       | 7         | 1.2         | 2015-01-02        | LHR  | London Heathrow Airport                          |
      | False       | 8         | 1.2         | 2015-01-02        | HKG  | Hong Kong International Airport                  |
      | False       | 9         | 1.2         | 2015-01-02        | PVG  | Shanghai Pudong International Airport            |
      | False       | 0         | 1.2         | 2015-01-03        | CDG  | Paris-Charles de Gaulle Airport	                |

    Then an exception is raised
