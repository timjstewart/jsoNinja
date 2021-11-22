import io
import sys
import logging

from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Iterator, Union
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import jsonpath
from jinja2 import Environment
import yaml

LOGGER = logging.getLogger(__name__)


JSON = Union[str, bool, List[Any], Dict[str, Any]]


class DataStore:

    def __init__(self) -> None:
        self._dict = defaultdict(dict)

    def set(self, key_path, value: JSON) -> None:
        obj, key = self._get_dict_and_key(key_path)
        obj[key] = value

    def append(self, key_path, value: JSON) -> None:
        obj, key = self._get_dict_and_key(key_path)
        if not isinstance(obj[key], list):
            obj[key] = []
        obj[key].append(value)

    def extend(self, key_path, value: JSON) -> None:
        obj, key = self._get_dict_and_key(key_path)
        if not isinstance(obj[key], list):
            obj[key] = []
        obj[key].extend(value)

    def _get_dict_and_key(self, key_path) -> Optional[Dict[str, JSON]]:
        keys = key_path.strip().split(".")
        curr = self._dict
        for key in keys[:-1]:
            if key not in curr:
                curr[key] = defaultdict(dict)
            curr = curr[key]
        if curr is not None:
            return curr, keys[-1]
        return None

    def __str__(self) -> str:
        s = io.StringIO()
        for key, value in self._dict.items():
            s.write(f"{key}: {value}\n")
        return s.getvalue()


@dataclass
class Line:
    number: int
    text: str


class Source(ABC):
    pass
    # @abstractmethod
    # def source(self) -> str:
    #     pass

    # def source_type(self) -> str:
    #     pass


class TextData(Source):
    @abstractmethod
    def lines(self) -> Iterator[Line]:
        ...


class String(TextData):
    """
    Text data that can be safely held in memory in a single string
    """
    def __init__(self, value: str) -> None:
        self.value = value

    def lines(self) -> Iterator[Line]:
        for i, line in enumerate(self.value.split('\n'), 1):
            yield Line(i, line)


class TextFile(TextData):
    """
    Text data that comes from a file where reading the entire file into memory
    may not be feasible.
    """
    def __init__(self, path: Path) -> None:
        self.path = path

    def lines(self) -> Iterator[Line]:
        with open(self.path, "r") as f:
            line_number = 0
            while True:
                line = f.readline()
                if line:
                    line_number += 1
                    yield Line(line_number, line)
                else:
                    break


class JsonDataSource(Source):
    @abstractmethod
    def root(self) -> JSON:
        ...


class JsonFile(JsonDataSource):
    def __init__(self, path: Path) -> None:
        self.path = path

    def root(self) -> JSON:
        return yaml.load(self.path.read_text(),
                         Loader=yaml.SafeLoader)


class JsonDict(JsonDataSource):
    def __init__(self, json_obj: JSON) -> None:
        self.json_obj = json_obj

    def root(self) -> JSON:
        return self.json_obj


@dataclass
class Sink:
    output_path: str
    template_path: Path

    def send(self, store: DataStore) -> None:
        LOGGER.info("sending data to sink: %s %s",
                    self.output_path,
                    self.template_path)


class JinjaTemplateSink(Sink):
    def __init__(self, template_path: Path, output_path: Path) -> None:
        self.template_path = template_path
        self.output_path = output_path


    def send(self, store: DataStore) -> None:
        env = Environment()
        template = env.from_string(self.template_path.read_text())
        result = template.render(**store._dict)
        self.output_path.write_text(result)



class JsonCollector(ABC):

    def __init__(self, json_path: str) -> None:
        """
        |param json_path| a JSONPath string that will be used to collect data.
        """
        self.json_path = json_path

    @abstractmethod
    def _set(self, data_store: DataStore, json_obj: JSON) -> None:
        ...

    def run(self, json_obj: JSON, data_store: DataStore) -> None:
        json_obj = jsonpath.jsonpath(json_obj, self.json_path)
        if json_obj is False:
            LOGGER.warning("Could not find path: %s", self.json_path)
        else:
            self._set(data_store, json_obj)


class JsonStaticListExtender(JsonCollector):
    def __init__(self, json_path: str, key_path: str) -> None:
        super().__init__(json_path)
        self.key_path = key_path

    def _set(self, data_store: DataStore, json_obj: JSON) -> None:
        data_store.extend(self.key_path, json_obj)


class JsonStaticListAppender(JsonCollector):
    def __init__(self, json_path: str, key_path: str) -> None:
        super().__init__(json_path)
        self.key_path = key_path

    def _set(self, data_store: DataStore, json_obj: JSON) -> None:
        data_store.append(self.key_path, json_obj)


class JsonPipe:
    def __init__(self, source: JsonDataSource, *collectors: List[JsonCollector]) -> None:
        self.source = source
        self.collectors = list(collectors)

    def run(self, store: DataStore) -> None:
        json_obj = self.source.root()
        for collector in self.collectors:
            collector.run(json_obj, store)


class Transformer:
    def __init__(self, sources: List[JsonPipe], sinks: List[Sink]) -> None:
        self.sources = list(sources)
        self.sinks = list(sinks)

    def run(self) -> None:
        store = DataStore()
        for source in self.sources:
            source.run(store)
        for sink in self.sinks:
            sink.send(store)


def main(_args: List[str]) -> None:
    xform = Transformer(
        sources=[
            JsonPipe(JsonFile(Path("examples/test.json")),
                     JsonStaticListExtender("$.[*].name", "data.people.names"))
            ],
        sinks=[
            JinjaTemplateSink(
                template_path=Path("./templates/people-text.jinja"),
                output_path=Path("./output/test.txt"))
            ])
    xform.run()


if __name__ == '__main__':
    main(sys.argv[1:])
