from abc import ABC, abstractmethod
from pandas import DataFrame


class DatasetTransformer(ABC):
    @staticmethod
    def for_dataset(dataset_uri: str):
        transformer = transformers_registry.get(dataset_uri)
        if not transformer:
            raise ValueError(f'No transformer found for dataset: {dataset_uri}')

        return transformer()

    def prepare_to_load(self, raw_data: str) -> bytes:
        cleaned = self._clean(raw_data)
        aggregated = self._aggregate(cleaned)

        # return aggregated.to_parquet(index=False)

    @abstractmethod
    def _clean(self, raw_data: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _aggregate(self, cleaned_data: DataFrame) -> DataFrame:
        raise NotImplementedError


class BikeTransformer(DatasetTransformer):
    def _clean(self, raw_data: str) -> DataFrame:
        pass

    def _aggregate(self, cleaned_data: DataFrame) -> DataFrame:
        pass


class ChargerTransformer(DatasetTransformer):
    def _clean(self, raw_data: str) -> DataFrame:
        pass

    def _aggregate(self, cleaned_data: DataFrame) -> DataFrame:
        pass


class RoadTransformer(DatasetTransformer):
    def _clean(self, raw_data: str) -> DataFrame:
        pass

    def _aggregate(self, cleaned_data: DataFrame) -> DataFrame:
        pass


transformers_registry = {
    'bike_points/raw/': BikeTransformer,
    'chargers/raw/': ChargerTransformer,
    'roads/raw/': RoadTransformer,
}