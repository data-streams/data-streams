package com.spbsu.flamestream.example.graph_search;

import com.spbsu.flamestream.core.graph.HashUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public enum SqliteOutboundEdges implements BreadthSearchGraph.HashedVertexEdges {
  INSTANCE;
  final Connection connection;
  final int minTail, maxTail;

  SqliteOutboundEdges() {
    try {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:/Users/faucct/Code/flamestream/twitter.sqlite");
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          connection.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }));
      {
        final ResultSet result = connection.createStatement().executeQuery("SELECT MIN(tail) AS tail FROM edges;");
        result.next();
        this.minTail = result.getInt("tail");
      }
      {
        final ResultSet result = connection.createStatement().executeQuery("SELECT MAX(tail) AS tail FROM edges;");
        result.next();
        this.maxTail = result.getInt("tail");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
    try {
      final Statement statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
      final ResultSet rs = statement.executeQuery("SELECT head FROM edges WHERE tail = " + vertexIdentifier.id);
      final List<BreadthSearchGraph.VertexIdentifier> outbound = new ArrayList<>();
      while (rs.next()) {
        // read the result set
        outbound.add(new BreadthSearchGraph.VertexIdentifier(rs.getInt("head")));
      }
      return outbound.stream();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int hash(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
    return HashUnit.scale(vertexIdentifier.id, minTail, maxTail);
  }
}
