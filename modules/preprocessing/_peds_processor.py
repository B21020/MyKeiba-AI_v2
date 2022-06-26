from sklearn.preprocessing import LabelEncoder
from ._abstract_data_processor import AbstractDataProcessor


class PedsProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        super().__init__(filepath)
    
    def _preprocess(self):
        df = self.raw_data
        for column in df.columns:
            df[column] = LabelEncoder().fit_transform(df[column].fillna('Na'))
        return df.astype('category')
 