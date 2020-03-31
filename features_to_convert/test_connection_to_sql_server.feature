Feature: Test Connection To SQL Server

  Scenario: Test Connection To SQL Server

    Given we create a Client instance
     When we run the test_sql_connection method
     Then the result is True
