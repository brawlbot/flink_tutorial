# wordcount

## 0. get env
- `env.getConfig().toString();`

## 1. parallelism

- set parallelism by `env.setParallelism(2);`
- check parallelism by `System.out.println(text.getExecutionConfig().toString());`

## 2. file source
```java
builder.monitorContinuously(Duration.ofMillis(1));
```
in shell
```bash
echo "w1" > input/input7.txt
```


# Multiple file source
- `FileSource.forRecordStreamFormat(new TextLineInputFormat(), input_filePath);`
- `builder.monitorContinuously(Duration.ofMillis(1));`
- `builder.monitorContinuously(Duration.ofSeconds(10));`
