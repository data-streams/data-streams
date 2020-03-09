package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.SerializableFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public enum SqliteOutboundEdges
        implements SerializableFunction<BreadthSearchGraph.VertexIdentifier, List<BreadthSearchGraph.VertexIdentifier>> {
  INSTANCE;
  final Connection connection;

  SqliteOutboundEdges() {
    try {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:twitter.sqlite");
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          connection.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
    try {
      final Statement statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
      final ResultSet rs = statement.executeQuery("SELECT head FROM edges WHERE tail = " + vertexIdentifier.id);
      final List<BreadthSearchGraph.VertexIdentifier> outbound = new ArrayList<>();
      while (rs.next()) {
        // read the result set
        outbound.add(new BreadthSearchGraph.VertexIdentifier(rs.getInt("head")));
      }
      return outbound;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
