from core.s3.s3_utils import S3Service
from external_api.services.flash_report.vin_decoder.config import WMI_TO_OEM


def load_model(model_name: str):
    s3 = S3Service()
    return s3.load_pickle(f"models/{model_name}")


class VinDecoder:
    """
    VinDecoder predicts MODEL_VERSION from a VIN using a pre-trained .pkl pipeline.
    """

    def __init__(self):
        """
        Initialize the decoder by loading a pre-trained .pkl pipeline.

        Parameters
        ----------
        model_path : str
            Path to the .pkl file containing the trained pipeline.
        """
        self.model = load_model("model_vin_decoder.pkl")

    def decode(self, vin: str) -> str:
        """
        Predict MODEL_VERSION for a single VIN.

        Parameters
        ----------
        vin : str
            VIN to decode.

        Returns
        -------
        str
            Predicted MODEL_VERSION.
        """

        predictions = self.model.predict([vin])[0]

        model = predictions.split("_")[0].capitalize()
        version = predictions.split("_")[1].capitalize()

        vin_first_three_characters = vin[:3]

        if vin_first_three_characters not in WMI_TO_OEM:
            return None, None

        if model.capitalize() == WMI_TO_OEM[vin_first_three_characters]:
            return model, version
        else:
            return WMI_TO_OEM[vin_first_three_characters], None

    def decode_batch(self, vins: list[str]) -> list[str]:
        """
        Predict MODEL_VERSION for a list of VINs.

        Parameters
        ----------
        vins : list[str]
            List of VINs.

        Returns
        -------
        list[str]
            Predicted MODEL_VERSIONs.
        """
        return self.model.predict(vins).tolist()
