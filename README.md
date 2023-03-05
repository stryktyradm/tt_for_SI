Активировать при необходимости виртуальное окружение:

```bash
python3 -m venv venv
```

Сгенерировать log файлы:

```bash
python3 log_generator.py <path/to/dir>
```

Например:

```bash
python3 log_generator.py ./data/
```

Объединить log файлы в один:
```bash
python3 <your_script>.py <path/to/log1> <path/to/log2> -o <path/to/merged/log>
```

Например, на основе предварительно сгенерированных файлов:
```bash
python3 merge_logfile.py data/log_a.jsonl data/log_b.jsonl -o data/out.jsonl
```