from abc import ABC, abstractmethod

import pyspark


class SparkStorageServiceOwner(ABC):

    @property
    @abstractmethod
    def spark(self) -> pyspark.sql.SparkSession:
        pass
