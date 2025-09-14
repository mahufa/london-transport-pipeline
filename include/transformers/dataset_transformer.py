from abc import ABC, abstractmethod
from pandas import DataFrame

from include.transformers.bike_points.bike_points import clean_bike_points


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

    def _aggregate(self, cleaned_data: DataFrame) -> DataFrame:
        """Only if needed"""
        return cleaned_data


class BikeTransformer(DatasetTransformer):
    def _clean(self, raw_data: str) -> DataFrame:
        return clean_bike_points(raw_data)


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