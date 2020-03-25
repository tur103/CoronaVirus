from pandas import DataFrame
import matplotlib.pyplot as plt

import constants


class LineProgressionVisualizer:
    """
    Visualizer for visualizing progression through line chart.
    """
    @staticmethod
    def visualize_progression(progression: DataFrame, country_region: str):
        """
        Creates a visualization of the deaths and confirmed cases progression with the given data of the chosen country.
        """
        deaths_dataframe = progression[progression[constants.CASE_TYPE] == constants.DEATHS]
        deaths_dataframe = deaths_dataframe[constants.NEGATIVE_NUMBER_OF_RECORDS_TO_SHOW:]
        plt.subplot(*constants.FIRST_SUB_PLOT_LOCATION)
        plt.plot(deaths_dataframe[constants.DATE], deaths_dataframe[constants.CASES], constants.LINE_TYPE)
        plt.title(constants.TITLE.format(case_type=constants.DEATHS, country_region=country_region))
        plt.xlabel(constants.DATE)
        plt.ylabel(constants.CASES)

        confirmed_dataframe = progression[progression[constants.CASE_TYPE] == constants.CONFIRMED]
        confirmed_dataframe = confirmed_dataframe[constants.NEGATIVE_NUMBER_OF_RECORDS_TO_SHOW:]
        plt.subplot(*constants.SECOND_SUB_PLOT_LOCATION)
        plt.plot(confirmed_dataframe[constants.DATE], confirmed_dataframe[constants.CASES], constants.LINE_TYPE)
        plt.title(constants.TITLE.format(case_type=constants.CONFIRMED, country_region=country_region))
        plt.xlabel(constants.DATE)
        plt.ylabel(constants.CASES)

        plt.show()
