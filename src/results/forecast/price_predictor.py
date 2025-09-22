
from typing import Optional, Union, List
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_percentage_error, mean_absolute_error, root_mean_squared_error
import numpy as np
from core.s3.s3_utils import S3Service
from core.gsheet_utils import load_excel_data
from core.sql_utils import get_sqlalchemy_engine

class CarPricePredictor:
    """
    Car price prediction model using RandomForest
    with automatic preprocessing of categorical variables.
    """
    
    def __init__(self,
                 n_estimators: int = 100,
                 max_depth: Optional[int] = None,
                 random_state: int = 42,
                 categorical_features: Optional[List[str]] = None):
        """
        Initialize the model.
        
        Args:
            n_estimators: Number of trees in the forest
            max_depth: Maximum depth of the trees
            random_state: Random seed for reproducibility
            categorical_features: List of categorical column names
        """
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.random_state = random_state
        self.categorical_features = categorical_features or ['make_name', 'battery_chemistry']
        self.feature_columns = ['make_name','autonomy','battery_chemistry','net_capacity', 'odometer', 'year', 'soh']

        # Model initialization
        self.model = RandomForestRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=self.random_state
        )

        # Preprocessor initialization
        self.preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), 
                 self.categorical_features)
            ],
            remainder='passthrough',
            force_int_remainder_cols=False
        )

        # Complete pipeline
        self.pipeline = Pipeline(steps=[
            ('preprocess', self.preprocessor),
            ('model', self.model)
        ])

        # Variables to store training information
        self.is_fitted = False
        self.feature_names_ = None
        self.target_column = None

    def fit(self, data: pd.DataFrame, y: Union[pd.Series, np.ndarray], target_column: str = 'price') -> 'CarPricePredictor':
        """

        Args:
            data: DataFrame with features
            y: Target variable (price)
            target_column: Name of the target column
            
        Returns:
            self: Trained model instance
        """
        try:
            # Data validation
            if data.empty:
                raise ValueError("Training data cannot be empty")

            if len(data) != len(y):
                raise ValueError("X and y must have the same number of samples")

            # Check if categorical columns exist
            missing_cats = [cat for cat in self.categorical_features if cat not in data.columns]
            if missing_cats:
                print(f"Warning: Missing categorical columns: {missing_cats}")
                self.categorical_features = [cat for cat in self.categorical_features if cat in data.columns]

                # Update preprocessor
                if self.categorical_features:
                    self.preprocessor = ColumnTransformer(
                        transformers=[
                            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False),
                             self.categorical_features)],
                        remainder='passthrough',
                        force_int_remainder_cols=False
                    )
                else:
                    # No categorical columns, use everything as is
                    self.preprocessor = ColumnTransformer(
                        transformers=[],
                        remainder='passthrough'
                    )

                self.pipeline = Pipeline(steps=[
                    ('preprocess', self.preprocessor),
                    ('model', self.model)
                ])

            print("Starting training...")
            self.pipeline.fit(data, y)

            self.feature_names_ = data.columns.tolist()
            self.target_column = target_column
            self.is_fitted = True

            print("Training completed successfully!")

            y_pred = self.pipeline.predict(data)
            mape = mean_absolute_percentage_error(y, y_pred)
            mae = mean_absolute_error(y, y_pred)
            rmse = root_mean_squared_error(y, y_pred)

            print("Training metrics:")
            print(f"  - MAE: {mae:.0f}")
            print(f"  - MAPE: {mape * 100:.2f}") 
            print(f"  - RMS: {rmse:.0f}")
            
            return self
            
        except Exception as e:
            print(f"Error during training: {str(e)}")
            raise
    
    def predict(self, data: pd.DataFrame) -> np.ndarray:
        if not self.is_fitted:
            raise ValueError("Model must be trained before making predictions. Call fit() first.")
        
        try:
            # Check that all necessary columns are present
            missing_features = [feat for feat in self.feature_names_ if feat not in data.columns]
            if missing_features:
                raise ValueError(f"Missing columns in prediction data: {missing_features}")

            # Select the right columns in the right order
            data_subset = data[self.feature_names_]

            # predictions
            predictions = self.pipeline.predict(data_subset)

            return predictions

        except Exception as e:
            print(f"Error during prediction: {str(e)}")
            raise
    
    def save(self, name: str) -> None:
        if not self.is_fitted:
            raise ValueError("Model must be trained before saving.")
        
        try:
            s3 = S3Service()
            s3.save_as_pickle(self, f"models/{name}")
        except Exception as e:
            print(f"Error during saving: {str(e)}")
            raise

    @classmethod
    def load(cls, name: str) -> 'CarPricePredictor':
        try:
            s3 = S3Service()
            model = s3.load_pickle(f"models/{name}")


            instance = cls(
                n_estimators=model.model.n_estimators,
                max_depth=model.model.max_depth,
                random_state=model.model.random_state,
                categorical_features=model.categorical_features
            )

            instance.pipeline = model.pipeline
            instance.feature_names_ = model.feature_names_
            instance.target_column = model.target_column
            instance.is_fitted = True

            print(f"Model loaded successfully from: {name}")
            return instance

        except Exception as e:
            print(f"Error during loading: {str(e)}")
            raise
    
    def get_feature_importance(self) -> pd.DataFrame:

        if not self.is_fitted:
            raise ValueError("Model must be trained to get feature importance.")

        # Get feature names after preprocessing
        feature_names = self.pipeline.named_steps['preprocess'].get_feature_names_out()
        importances = self.pipeline.named_steps['model'].feature_importances_

        # Create DataFrame
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importances
        }).sort_values('importance', ascending=False)

        return importance_df

    def evaluate(self, data_test: pd.DataFrame, target_test: Union[pd.Series, np.ndarray]) -> dict:

        if not self.is_fitted:
            raise ValueError("Model must be trained before evaluation.")

        y_pred = self.predict(data_test)

        mae = mean_absolute_error(target_test, y_pred)
        mape = mean_absolute_percentage_error(target_test, y_pred)
        rmse = root_mean_squared_error(target_test, y_pred)    
        metrics = {
            'RMSE : ': round(rmse),
            'MAE : ': round(mae),
            'MAPE : ': round(mape, 2) * 100
        }

        return metrics
    
    def get_data(self) -> pd.DataFrame:
        """
        Load and preprocess data for training.
        - Reads Excel scrap data
        - Joins with database vehicle model info
        - Cleans and formats columns
        """
        df_scrapping = load_excel_data("Courbes de tendance", "Courbes OS")
        df_scrapping = pd.DataFrame(columns=df_scrapping[:1][0], data=df_scrapping[1:])
        df_scrapping = df_scrapping.rename(columns={'OEM': 'make_name', 'SoH':'soh', 'Odomètre (km)': 'odometer', 'Année': 'year'})  # lowercase for consistency

        engine = get_sqlalchemy_engine()
        df_dbeaver = pd.read_sql(
            """
            SELECT vm.model_name, vm.type, vm.version, vm.autonomy,
                   b.battery_chemistry, b.capacity, b.net_capacity
            FROM vehicle_model vm
            JOIN battery b ON b.id = vm.battery_id
            """,
            engine
        )

        df_scrapping['Modèle'] = df_scrapping['Modèle'].apply(lambda x: x.lower())
        # Merge sources
        df_info = df_scrapping.merge(
            df_dbeaver,
            right_on=["model_name", "type"],
            left_on=['Modèle', 'Type'],
            how='left'
        )[
            ['make_name', 'Modèle', 'Type', 'version', 'autonomy',
             'battery_chemistry', 'net_capacity',
             'odometer', 'year', 'soh', 'price']
        ].drop_duplicates(subset=['make_name', 'Modèle', 'odometer', 'year', 'soh', 'price'])

        # Clean numeric values
        df_info['price'] = df_info['price'].replace('', np.nan).astype(float)
        df_info['soh'] = df_info['soh'].apply(lambda x: float(str(x).replace('%', '')))
        df_info['odometer'] = df_info['odometer'].apply(lambda x: float(str(x).replace(',', '').replace(' ', '')))
        df_info['year'] = df_info['year'].astype(int)

        # Drop rows missing essential values
        df_info = df_info.dropna(subset=['price', 'net_capacity']).reset_index(drop=True)
        return df_info

    def run(self, model_name: str, target_column: str = 'price') -> None:
        """
        End-to-end pipeline to fetch data, train the model, and save it to S3.

        Args:
            model_name: Name under which the trained model will be saved in S3
            target_column: Column name for the target variable
        """
        try:
            print("Fetching training data...")
            data = self.get_data()
            if data is None or data.empty:
                raise ValueError("No training data received from _get_data()")

            if target_column not in data.columns:
                raise ValueError(f"Target column '{target_column}' not found in dataset")

            # Separate features and target
            X = data[self.feature_columns]
            y = data[target_column]

            print("Training model...")
            self.fit(X, y, target_column=target_column)

            print("Saving model to S3...")
            self.save(model_name)

            print(f"Run completed successfully. Model saved as '{model_name}'")

        except Exception as e:
            print(f"Error in run(): {str(e)}")
            raise


