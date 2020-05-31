package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.core.graph.HashGroup;
import com.spbsu.flamestream.core.graph.HashUnit;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.NewRear;
import com.spbsu.flamestream.runtime.graph.state.GroupGroupingState;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Commit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.GimmeLastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.LastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepared;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Ready;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.HashUnitMap;
import com.spbsu.flamestream.runtime.utils.collections.ListHashUnitMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GraphManager extends LoggingActor {
  private final Graph graph;
  private final ActorRef registryHolder;
  private final ActorRef committer;
  private final ComputationProps computationProps;
  private final StateStorage storage;
  private final String nodeId;
  private final @Nullable
  ActorRef localAcker;

  private ActorRef sourceComponent;
  private final List<ActorRef> sinkComponents = new ArrayList<>();

  private final HashUnitMap<ActorRef> routes = new ListHashUnitMap<>();
  private final HashMap<String, Graph.Vertex> vertexById = new HashMap<>();
  private final Map<String, HashUnitMap<ActorRef>> verticesComponents;
  private final Set<ActorRef> components = new HashSet<>();

  private final Map<HashUnit, Map<String, GroupingState>> unitStates = new HashMap<>();
  private final TObjectIntMap<String> groupingWindows = new TObjectIntHashMap<>();

  private GraphManager(
          String nodeId,
          Graph graph,
          @Nullable ActorRef localAcker,
          ActorRef registryHolder,
          ActorRef committer,
          ComputationProps computationProps,
          StateStorage storage
  ) {
    this.nodeId = nodeId;
    this.storage = storage;
    this.computationProps = computationProps;
    this.graph = graph;
    this.localAcker = localAcker;
    this.registryHolder = registryHolder;
    this.committer = committer;
    graph.components().forEach(component -> component.forEach(vertex -> vertexById.put(vertex.id(), vertex)));
    verticesComponents = graph.components().flatMap(Function.identity()).collect(Collectors.toMap(
            Graph.Vertex::id,
            __ -> new ListHashUnitMap<>()
    ));
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    if (localAcker != null) {
      localAcker.tell(new MinTimeUpdateListener(self()), self());
    }
  }

  public static Props props(
          String nodeId,
          Graph graph,
          @Nullable ActorRef localAcker,
          ActorRef registryHolder,
          ActorRef committer,
          ComputationProps layout,
          StateStorage storage) {
    return Props.create(GraphManager.class, nodeId, graph, localAcker, registryHolder, committer, layout, storage)
            .withDispatcher("processing-dispatcher");
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              final Map<HashUnit, ActorRef> routerMap = new HashMap<>();
              computationProps.hashGroups()
                      .forEach((key, value) -> value.units()
                              .forEach(unit -> routerMap.put(unit, (ActorRef) managers.get(key))));
              routes.putAll(routerMap);

              registryHolder.tell(new GimmeLastCommit(), self());
              getContext().become(deploying());
            })
            .matchAny(m -> stash())
            .build();
  }

  private static List<HashGroup> partitionedHashGroup(HashGroup toPartition, int partitions) {
    final List<Set<HashUnit>> partitionHashUnits = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      partitionHashUnits.add(new HashSet<>());
    }
    for (final HashUnit unit : toPartition.units()) {
      for (int i = 0; i < partitions; i++) {
        partitionHashUnits.get(i).add(new HashUnit(
                unit.scale(i, 0, partitions - 1),
                unit.scale(i + 1, 0, partitions - 1) - 1
        ));
      }
    }
    return partitionHashUnits.stream().map(HashGroup::new).collect(Collectors.toList());
  }

  private Receive deploying() {
    return ReceiveBuilder.create()
            .match(LastCommit.class, lastCommit -> {
              log().info("Received last commit '{}'", lastCommit);
              final int partitions = computationProps.partitions();
              final List<Map<String, GroupGroupingState>> partitionStateByVertex = new ArrayList<>();
              final List<HashGroup> partitionHashGroup =
                      partitionedHashGroup(computationProps.hashGroups().get(nodeId), partitions);
              for (final HashGroup hashGroup : partitionHashGroup) {
                final HashMap<String, GroupGroupingState> stateByVertex = new HashMap<>();
                partitionStateByVertex.add(stateByVertex);
                for (final HashUnit unit : hashGroup.units()) {
                  final Map<String, GroupingState> unitState = storage.stateFor(
                          unit,
                          lastCommit.globalTime()
                  );
                  graph.components()
                          .flatMap(vertexStream -> vertexStream)
                          .filter(vertex -> vertex instanceof Grouping)
                          .forEach(vertex -> {
                            final Grouping<?> grouping = (Grouping<?>) vertex;
                            unitState.computeIfAbsent(vertex.id(), __ -> new GroupingState(grouping));
                            groupingWindows.put(vertex.id(), grouping.window());
                          });
                  unitState.forEach((vertexId, groupingState) ->
                          stateByVertex.computeIfAbsent(vertexId, __ -> new GroupGroupingState(groupingState.grouping))
                                  .addUnitState(unit, groupingState)
                  );
                  unitStates.put(unit, unitState);
                }
              }

              graph.components().forEach(c -> {
                final Set<Graph.Vertex> vertexSet = c.collect(Collectors.toSet());

                for (int partition = 0; partition < partitions; partition++) {
                  final HashGroup hashGroup = partitionHashGroup.get(partition);
                  final Map<String, GroupGroupingState> stateByVertex = partitionStateByVertex.get(partition);
                  final ActorRef component = context().actorOf(Component.props(
                          nodeId,
                          vertexSet,
                          graph,
                          routes,
                          hashGroup,
                          localAcker,
                          computationProps,
                          stateByVertex
                  ));

                  for (Graph.Vertex dest : vertexSet) {
                    final HashUnitMap<ActorRef> actorRefHashUnitMap = verticesComponents.get(dest.id());
                    for (final HashUnit unit : hashGroup.units()) {
                      actorRefHashUnitMap.put(unit, component);
                    }
                  }

                  components.add(component);

                  if (sourceComponent == null) {
                    vertexSet.stream()
                            .filter(v -> v instanceof Source)
                            .findAny()
                            .ifPresent(v -> sourceComponent = component);
                  }

                  vertexSet.stream()
                          .filter(v -> v instanceof Sink)
                          .findAny()
                          .ifPresent(v -> sinkComponents.add(component));
                }
              });

              committer.tell(new Ready(), self());
              unstashAll();
              getContext().become(managing());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive managing() {
    return ReceiveBuilder.create()
            .match(DataItem.class, dataItem -> sourceComponent.forward(dataItem, context()))
            .match(AddressedItem.class, addressed -> {
              final HashUnitMap<ActorRef> hashedComponents = verticesComponents.get(addressed.vertexId);
              final Graph.Vertex vertex = vertexById.get(addressed.vertexId);
              if (addressed.type == AddressedItem.Type.BROADCAST && hashedComponents.entrySet().size() != 1) {
                int childId = 0;
                for (final Map.Entry<HashUnit, ActorRef> route : hashedComponents.entrySet()) {
                  final DataItem item = addressed.item().cloneWith(new Meta(
                          addressed.item().meta(),
                          0,
                          childId++
                  ));
                  ack(vertex, item);
                  route.getValue().forward(new AddressedItem(item, addressed), context());
                }
                ack(vertex, addressed.item());
              } else if (addressed.type == AddressedItem.Type.HASHED) {
                hashedComponents.get(addressed.hash).forward(addressed, context());
              } else {
                hashedComponents.first().forward(addressed, context());
              }
            })
            .match(MinTimeUpdate.class, this::onMinTime)
            .match(Prepare.class, this::onPrepare)
            .match(Commit.class, commit -> sourceComponent.forward(commit, context()))
            .match(NewRear.class, newRear -> sinkComponents.forEach(component -> component.forward(newRear, context())))
            .match(Heartbeat.class, gt -> sourceComponent.forward(gt, context()))
            .match(UnregisterFront.class, u -> sourceComponent.forward(u, context()))
            .build();
  }

  private void ack(Graph.Vertex vertex, DataItem item) {
    if (localAcker != null) {
      final GlobalTime globalTime = item.meta().globalTime();
      localAcker.tell(new Ack(
              graph.trackingComponent(vertex).index,
              new GlobalTime(globalTime.time(), globalTime.frontId()),
              item.xor()
      ), self());
    }
  }

  private void onPrepare(Prepare prepare) {
    final CompletableFuture<?>[] futures = new CompletableFuture[components.size()];
    int index = 0;
    for (ActorRef component : components) {
      futures[index++] = PatternsCS.ask(component, prepare, FlameConfig.config.bigTimeout()).toCompletableFuture();
    }
    CompletableFuture.allOf(futures).thenRun(() -> {
      unitStates.forEach((hashUnit, stateMap) -> storage.putState(
              hashUnit,
              prepare.globalTime(),
              stateMap.entrySet()
                      .stream()
                      .collect(Collectors.toMap(
                              Map.Entry::getKey,
                              e -> e.getValue()
                                      .subState(prepare.globalTime(), groupingWindows.get(e.getKey()))
                      ))
      ));
      committer.tell(new Prepared(), self());
    });
  }

  private void onMinTime(MinTimeUpdate minTimeUpdate) {
    components.forEach(c -> c.tell(minTimeUpdate, sender()));
  }
}
