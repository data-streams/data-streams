package com.spbsu.flamestream.example.labels;

import scala.Tuple2;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class BreadthSearchGraph {
  static class VertexIdentifier {
  }

  static abstract class Input {
  }

  static class Request extends Input {
    static class Identifier {
    }

    final Identifier identifier;
    final VertexIdentifier vertexIdentifier;
    final int pathLength;

    Request(
            Identifier identifier,
            VertexIdentifier vertexIdentifier,
            int pathLength
    ) {
      this.identifier = identifier;
      this.vertexIdentifier = vertexIdentifier;
      this.pathLength = pathLength;
    }
  }

  static class VertexEdgesUpdate extends Input {
    final VertexIdentifier source;
    final List<VertexIdentifier> targets;

    VertexEdgesUpdate(VertexIdentifier source, List<VertexIdentifier> targets) {
      this.source = source;
      this.targets = targets;
    }
  }

  static class RequestKey implements Label {
    final Request.Identifier identifier;

    RequestKey(Request.Identifier identifier) {this.identifier = identifier;}
  }

  private static class RequestOutput {
    final Request.Identifier requestIdentifier;
    final VertexIdentifier vertexIdentifier;

    private RequestOutput(Request.Identifier requestIdentifier, VertexIdentifier vertexIdentifier) {
      this.requestIdentifier = requestIdentifier;
      this.vertexIdentifier = vertexIdentifier;
    }
  }

  private static class Agent {
    final Request.Identifier requestIdentifier;
    final VertexIdentifier vertexIdentifier;
    final int remainingPathLength;

    enum ActionAfterVisit {
      Stop,
      VisitFirstTime,
      Revisit
    }

    Agent(
            Request.Identifier requestIdentifier,
            VertexIdentifier vertexIdentifier,
            int remainingPathLength
    ) {
      this.requestIdentifier = requestIdentifier;
      this.vertexIdentifier = vertexIdentifier;
      this.remainingPathLength = remainingPathLength;
    }
  }

  public static void main(String[] args) {
    mutableFlow(new HashMap<>());
  }

  protected static Flow<Request, Either<RequestOutput, RequestKey>> immutableFlow(
          Map<VertexIdentifier, List<VertexIdentifier>> vertexEdges
  ) {
    final Operator.Input<Request> requestInput = new Operator.Input<>();
    final Operator.Input<Agent> agentInput = new Operator.Input<>(Collections.singleton(RequestKey.class));
    agentInput.link(
            requestInput
                    .spawnLabel(RequestKey.class, request -> new RequestKey(request.identifier))
                    .map(request -> new Agent(request.identifier, request.vertexIdentifier, request.pathLength))
    );
    final Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit =
            agentAndActionAfterVisit(agentInput);
    agentInput.link(agentAndActionAfterVisit.flatMap(agent -> {
      if (agent._2 == Agent.ActionAfterVisit.Stop) {
        return Stream.empty();
      }
      return vertexEdges.getOrDefault(agent._1.vertexIdentifier, Collections.emptyList())
              .stream()
              .map(vertexIdentifier -> new Agent(
                      agent._1.requestIdentifier,
                      vertexIdentifier,
                      agent._1.remainingPathLength - 1
              ));
    }));
    final Operator.Input<Either<RequestOutput, RequestKey>> output = new Operator.Input<>();
    output.link(agentAndActionAfterVisit.flatMap(agent -> {
      if (agent._2 == Agent.ActionAfterVisit.VisitFirstTime) {
        return Stream.of(new Left<>(new RequestOutput(agent._1.requestIdentifier, agent._1.vertexIdentifier)));
      }
      return Stream.empty();
    }));
    output.link(agentAndActionAfterVisit.labelMarkers(RequestKey.class).map(Right::apply));
    return new Flow<>(requestInput, output);
  }

  protected static Flow<Input, Either<RequestOutput, RequestKey>> mutableFlow(
          Map<VertexIdentifier, List<VertexIdentifier>> vertexEdges
  ) {
    final Operator.Input<Input> requestInput = new Operator.Input<>();
    final Operator.Input<Agent> agentInput = new Operator.Input<>(Collections.singleton(RequestKey.class));
    agentInput.link(
            requestInput
                    .flatMap(input -> input instanceof Request ? Stream.of(((Request) input)) : Stream.empty())
                    .spawnLabel(RequestKey.class, request -> new RequestKey(request.identifier))
                    .map(request -> new Agent(request.identifier, request.vertexIdentifier, request.pathLength))
    );
    requestInput
            .flatMap(input ->
                    input instanceof VertexEdgesUpdate ? Stream.of((VertexEdgesUpdate) input) : Stream.empty()
            );
    final Operator.Input<Either<Agent, VertexEdgesUpdate>> vertexEdgesInput = new Operator.Input<>();
    final Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit =
            agentAndActionAfterVisit(agentInput);
    agentInput.link(vertexEdgesInput
            .keyedBy(either -> either.isLeft() ? either.left().get().vertexIdentifier : either.right().get().source)
            .<List<VertexIdentifier>, Agent>statefulFlatMap((either, edges) -> {
              if (either.isLeft()) {
                final Agent agent = either.left().get();
                return new Tuple2<>(
                        edges,
                        (edges == null ? vertexEdges.getOrDefault(
                                agent.vertexIdentifier,
                                Collections.emptyList()
                        ) : edges).stream().map(vertexIdentifier -> new Agent(
                                agent.requestIdentifier,
                                vertexIdentifier,
                                agent.remainingPathLength - 1
                        ))
                );
              } else {
                return new Tuple2<>(either.right().get().targets, Stream.empty());
              }
            }, Collections.singleton(RequestKey.class)));
    final Operator.Input<Either<RequestOutput, RequestKey>> output = new Operator.Input<>();
    output.link(agentAndActionAfterVisit.flatMap(agent -> {
      if (agent._2 == Agent.ActionAfterVisit.VisitFirstTime) {
        return Stream.of(new Left<>(new RequestOutput(agent._1.requestIdentifier, agent._1.vertexIdentifier)));
      }
      return Stream.empty();
    }));
    output.link(agentAndActionAfterVisit.labelMarkers(RequestKey.class).map(Right::apply));
    return new Flow<>(requestInput, output);
  }

  private static Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit(
          Operator.Input<Agent> agentInput) {
    return agentInput
            .filter(agent -> agent.remainingPathLength > 0)
            .keyedBy(agent -> agent.vertexIdentifier)
            .<Integer, Tuple2<Agent, Agent.ActionAfterVisit>>statefulMap(
                    (agent, remainingPathLength) -> {
                      final Agent.ActionAfterVisit actionAfterVisit;
                      if (remainingPathLength == null) {
                        actionAfterVisit = Agent.ActionAfterVisit.VisitFirstTime;
                        remainingPathLength = agent.remainingPathLength;
                      } else if (remainingPathLength < agent.remainingPathLength) {
                        actionAfterVisit = Agent.ActionAfterVisit.Revisit;
                        remainingPathLength = agent.remainingPathLength;
                      } else {
                        actionAfterVisit = Agent.ActionAfterVisit.Stop;
                      }
                      return new Tuple2<>(remainingPathLength, new Tuple2<>(agent, actionAfterVisit));
                    },
                    Collections.singleton(RequestKey.class)
            );
  }
}
