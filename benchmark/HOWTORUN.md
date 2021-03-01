# Че делать
1. Заполнить переменные терраформа в `terrafrom.tfvars`
2. `terraform apply` - создаст тачки
3. `terraform output --json | python ansible/tf2inventory.py > ansible/aws.yml` собрать инвенторий для ansible
4. `cd ../ && ./run_benchmarks.sh` - соберет флинк, положит в нужную папочку и запустит ансибл
5. `ssh -L 8081:localhost:8081 ubuntu@${manager_host}` - прокинет морду флинка на локальный порт. Там можно следить за статусом графа и читать логи ошибок

# Что готово
- `benchmark/flink-benchmark/src/main/java/com/spbsu/benchmark/flink/roundrobin/FlinkBench.java` - собран базовый граф, которы можно запускать локально. Прям дергать мейн и все будет работать
- Процедура выше все собирает, деплоит и разворачивает кластер флинка, в морде видны все тачки и статус графа

# Что надо сделать
Сейчас при старте графа возникает конфликт классов в районе Kryo, ошибку можно найти в морде флика в разделе "Job Manager -> Logs"
```
Caused by: java.lang.LinkageError: loader constraint violation: when resolving method 'org.objenesis.strategy.InstantiatorStrategy com.esotericsoftware.kryo.Kryo.getInstantiatorStrategy()' the class loader org.apache.flink.util.ChildFirstClassLoader @efd56a1 of the current class, com/spbsu/benchmark/flink/roundrobin/ops/SimpleSource, and the class loader 'app' for the method's defining class, com/esotericsoftware/kryo/Kryo, have different Class objects for the type org/objenesis/strategy/InstantiatorStrategy used in the signature (com.spbsu.benchmark.flink.roundrobin.ops.SimpleSource is in unnamed module of loader org.apache.flink.util.ChildFirstClassLoader @efd56a1, parent loader 'app'; com.esotericsoftware.kryo.Kryo is in unnamed module of loader 'app')
	at com.spbsu.benchmark.flink.roundrobin.ops.SimpleSource.open(SimpleSource.java:46) ~[?:?]
```

Надо разобраться, что с ним не так, починить и запустить. Дальше все должно быть как по маслу. Если не получится, то можно отбенчмаркать локально.

# Мысли
- Не обязательно убивать флинк на каждый бенчмарк в тасках флинка можно заомментировать убийство джоб менеджера и таск менеджера
- Всякая параметризайия живет в `bench.conf.j2`
- Я подхачил `BenchStandComponentFactory`, не знаю, как задать разным истокам разные айдишники, поэтому мы round-robin'ом отсылаем элементы с бенч-стенда на истоки
