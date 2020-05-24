package com.spbsu.flamestream.example.graph_search;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class EdgesToSqlite {
  public static void main(String[] args) throws Exception {
    try (
            Connection conn = DriverManager.getConnection("jdbc:sqlite:/Users/nikitasokolov/Downloads/twitter.sqlite");
            Statement stmt = conn.createStatement()
    ) {
      // create a new table
      stmt.execute("CREATE TABLE IF NOT EXISTS edges (tail integer3 NOT NULL, head integer3 NOT NULL);");
    }
  }
}
