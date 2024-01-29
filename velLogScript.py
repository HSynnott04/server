# coding: utf-8
import csv
import argparse
import sys
import os
import enum
import datetime
import constants
import charter
import statistics as n
#import matplotlib.pyplot as plt
#import xlwings as xw

""" VEL log analyzer

This is a script to analyze VEL log csv files.

It currently only accepts 2 .csv files for the input and output files respectively.
It assumes that the first row is the column headers and that different runs in the same file
have unique batch ids.

This script contains the following functions:
    * main - The main function of the script
    * calculateRunDuration - Calculates the duration of the run for the given batch in minutes as the time from the first entry of WorkSM = Wrk_runDeflectorDownSM to the last entry of WorkSM = Wrk_UnlockDoor.
    * calculateDryingDuration - Calculates the duration of the drying state for the given batch DrySM = Drying (in minutes)
    * calculateTotalMassProc - Calculate the total mass processed in the given batch as the initial mass minus the ending mass where initial mass is the average of SCALE for the first 5 seconds of the state WorkSM = Wrk_runDeflectorDownSM and the ending mass is the average of SCALE for the first 5 seconds of the state WorkSM = Wrk_runDeflectorUpSM.
    * calculateEnclosureIntegrityEndingPressure - Calculate the enclosure integrity ending pressure for the given batch as the average of the last 2 seconds of PT07 when WorkSM = Wrk_runCassettecheckSM and PV05 = 1
    * calculateAerosolIntegrityEndingPressure - Calculate the aerosol integrity ending pressure for the given batch as the average of PT03 over the last 2 seconds of the state WorkSM = Wrk_runAerosolCheckSM.
    * TODO this is actually referencing PT03? calculatePrePDCIntegrityEndingPressure - Calculate the pre PDC integrity ending pressure for the given batch as the average of PT06 over the last two seconds when WorkSM = Wrk_runDispPrecheckSM.
    * calculatePrePDCIntegrityAverageLeakRate - Calculate the pre PDC integrity average leak rate for the given batch as the average of FS01 over the duration when WorkSM = Wrk_runDispPrecheckSM, PV05 = 1, and PV07 = 1
    * calculateInitialExhaustTempSpikes - Calculates the initial exhaust temperature spikes for TT06 and TT07 respectively for the given batch over the interval when WorkSM = Wrk_runDryingSM and DrySM = Priming.
    * calculateDryingStatistics - Calculates the statistics based on the interval for the given batch when DrySM = Drying. This includes the minimum exhaust temperature, average plasma flow rate, peak plasma flow rate, minimum plenum pressure, peak plenum pressure, minimum aerosol pressure, peak aerosol pressure, minimum drying chamber pressure for PT08, peak drying chamber pressure for PT08, minimum drying chamber pressure for PT09, and peak drying chamber pressure for PT09.
    * calculateEndingExhaustTempSpikes - Calculates the ending exhaust temperature spike for TT06 and TT07 respecitvely for the given batch as their respective max over the interval when DrySM = DryingGasContinue.
    * calculatePostPDCIntegrityEndingPressure - Calculates the post PDC integrity ending pressure for the given batch as the average of PT06 over the last 2 seconds when WorkSM = Wrk_runDispIntegritySM and PV05 and PV07 = 1. 
    * calculatePostPDCIntegrityAverageLeakRate - Calculates the post PDC integrity average leak rate for the given batch as the average of FS01 over the interval when WorkSM = Wrk_runDispIntegritySM and PV05 and PV07 = 1.
    * calculateDuration - Calculate the duration in minutes from the given index of a start row to the given index of an end row.
    * getFirstSeconds - Calculate the index for the end row of the interval starting at startInd that goes for time seconds.
    * getLastSeconds - Calculate the index for the start row of the interval starting at endInd that goes for time seconds.
    * average - Calculate the average value for the given col from startInd to endInd. It is assumed that all values in this interval in col can be automatically converted to floats.
    * getIndicesWithPVConditions - Calculate the start and end indices for the given col (WorkSM value) in the given batch that have the specified values for PV05 and/or PV07. If both PV05 and PV07 are false, then this should provide the same values as the starts and endsList do. Currently this function only works for getting a WorkSM interval with PV conditions.
    * matchPV05PV07 - Determine if the PV conditions are met by the digital output of the given row. Only checks if PV05 and PV07 are set 1 to if given as true, does not check if set to 0 if given as false.
    * getDateTime - Get the DateTime object for the TimeStamp field.
    * areDifferentRuns - Takes two timestamp strings and returns true if the second is more than 1 minute past the first, indicating a different run has started.
"""


class velLogScript:
    def __init__(self):
        self.startsList : list = [] # The start indices for each WorkSM state in each batch in the input file.
        self.endsList : list = [] # The end indices for each WorkSM state in each batch in the input file.
        self.startsDryList : list = [] # The start indices for each DrySM state in each batch in the input file.
        self.endsDryList : list = [] # The end indices for each DrySM state in each batch in the input file.
        self.rows : list = [] # The list of rows that have been processed from the input file. This list begins with the first data row, not the header row. Each row is a dictionary with the keys being the header of the input file.
    # Enum for dataset columns used for calculating statistics

    #generator function for selecting rows with numbers as the ID, yields a row with a numerical id
    def getOnlyRowsWithNumericalID(self, data): 
        x : int
        for row in data:
            try:
                x = float(row["id"]) #this conversion will only be succesful if id is a number 
                yield row
            except:
                pass

    
    #This will return a list of all of the sequence IDs which reached the drying stage - Henry 18/1/2024
    def getSequenceIDOfAllSuccesses(self, data : list):
        result : list = []

        for row in data:
            try:
                if row['Workflow'] == constants.WorkState.runDrying.value and row['SeqId'] not in result:
                    result.append(row['SeqId'])
                
                
            except:
                #this is the old name for the workflow column, kept for backwards compatability
                print("Column header 'Workflow' not detected, trying WorkSM")
                if row['WorkSM'] == constants.WorkState.runDrying.value and row['SeqId'] not in result:
                    result.append(row['SeqId'])
        
        return result
    

    def callByCLI(self):
        parser = argparse.ArgumentParser(description='Take input and output csv files.')
        #metavar is name of arg on command line, type is default str?
        parser.add_argument('input_file', metavar='I', type=str, nargs=1, help='a csv log file to analyze')

        #will set args.input_file to name of csv
        args = parser.parse_args()
        in_file = args.input_file[0]
        print("Processing file ", in_file)
        
        if(in_file[-4:] != '.csv'):
            print("Please provide a .csv files for the input log file")
            exit
        self.main(in_file)

    def main(self, nameOfFile):
        print("Not for clinical use.")
        """
        The main function for this script. Assumes that command-line arguments have already been passed but not checked.
        """

        batch = nameOfFile[10:15]
        with open(nameOfFile, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            
            sortedReader = sorted(self.getOnlyRowsWithNumericalID(reader), key=lambda d : float(d['id'])) #there was a problem where data was coming in and being added to the log sheet out of order, this line will sort the data so the id column is in ascending sequential order
            
            workingSeqIDs = self.getSequenceIDOfAllSuccesses(sortedReader)
            print("All succesful sequence ID's have been found as ", workingSeqIDs)

            allrows : list = []
            fullRow = True # indicate whether all fields are provided for a row. If false, try and combine with the next row
            currentRow = {} # The currentRow being compiled. May contain multiple rows of the csv if they get shifted.
            
            for temprow in sortedReader:
                if fullRow:
                        currentRow = temprow
                    #try to convert row values to float, if it can't be converted and isn't empty then keep it as a string (for text fields)
                for key in temprow:
                    try:
                        currentRow[key] = float(temprow[key])
                    except:
                        if temprow[key] != None and temprow[key] != "":
                            currentRow[key] = temprow[key]

                
                fullRow = True

                for key in currentRow:
                    fullRow = fullRow and currentRow[key] != None
                # If a row is complete, iterate the index.
                if fullRow:
                    allrows.append(currentRow)
                    
            

            #this loop accounts for there being multiple runs which reach the drying stage in 1 csv file
            #each sequence which is succesful will have its own output file. Henry 18/1/2024
            for sequence in workingSeqIDs:  
                
                #reset all instance variables
                self.startsList = []
                self.endsList = [] 
                self.startsDryList = []
                self.endsDryList = [] 
                self.rows = []

                lastCategory : str = ""
                lastDryCategory : str = ""
                initialTimeStamp : str = "-1" # Initial value of lastTimeStamp to differentiate from shifted row.
                lastTimeStamp : str = initialTimeStamp
                uniqueNonzeroFaultCodes = []
                wasStopped = True # indicate whether the current row has been in an interval where WorkSM=Stopped
                fullRow = True # indicate whether all fields are provided for a row. If false, try and combine with the next row
                currentRow = {} # The currentRow being compiled. May contain multiple rows of the csv if they get shifted.
                index : int = 0 # Index is the current row index being processed. 0 starts at the first row of data, does not include header row.
                run : int = -1 # Current run number. Set at 0 for initial batch. Add 1 to this value to get the number of batches that have been or are being processed at that time.
                out_file_name : str = nameOfFile[:-4]+ "_" + sequence + "_out.xlsx"

                
                for row in allrows:

                    if row['SeqId'] != float(sequence):
                        continue
                    
                    if fullRow:
                        currentRow = row
                    #try to convert row values to float, if it can't be converted and isn't empty then keep it as a string (for text fields)
                    for key in row:
                        try:
                            currentRow[key] = float(row[key])
                        except:
                            if row[key] != None and row[key] != "":
                                currentRow[key] = row[key]
                                
                    currentTimeStamp = (row[constants.Column.timeStamp.value])

                    isDifferentRunTimestamp = currentTimeStamp != None and (lastTimeStamp == initialTimeStamp or self.areDifferentRuns(lastTimeStamp, currentTimeStamp))
                    isStoppedString = row[constants.Column.workState.value]
                    isStopped = isStoppedString == constants.WorkState.stopped.value if isStoppedString != None else wasStopped
                    # Initialization for new run
                    if isDifferentRunTimestamp or (isStopped ^ wasStopped):
                        if wasStopped or isDifferentRunTimestamp: # Cover both cases for starting of timestamp or start.
                            run += 1 #this does not happen more than once on the files that I'm trying, now that we do one run at a time. for all intents run is the same 
                            # Set up output file name based on dryer serial number and first timestamp of run.
                            # out_File_Time_Format = "_%Y%m%d_%H%M_full_log"
                            #dryerSerialNumber = input("Enter dryer serial number for run starting at " + currentTimeStamp + ": ")
                            # currentTime = getDateTime(currentTimeStamp)
                            #out_files.append(nameOfFile[:-4]+ "_out.xlsx") 
                            # Set up new tracking dictionaries for summary information.
                            self.startsList.append({})
                            self.endsList.append({})
                            self.startsList[run][constants.Column.batch.value] = index
                            self.startsDryList.append({})
                            self.endsDryList.append({})
                            uniqueNonzeroFaultCodes.append([])
                        elif isStopped: # Handle end case when separated by stopped.
                            self.endsList[run][constants.Column.batch.value] = index - 1
                        if isDifferentRunTimestamp and not(wasStopped): # Handle end case when timestamps are different and the end may have to be set at the same time as the start.
                            self.endsList[run-1][constants.Column.batch.value] = index - 1
                        
                    wasStopped = isStopped
                    
                    lastTimeStamp = currentTimeStamp if currentTimeStamp != None else lastTimeStamp
                    # Track fault codes for each run
                    try:
                        currentFaultCode = str(int(row[constants.Column.faultCode.value])) 
                    except: # Assume no errors if error code is empty or non-numeric.
                        currentFaultCode = "0"
                    if currentFaultCode != "0" and currentFaultCode not in uniqueNonzeroFaultCodes[run]:
                        uniqueNonzeroFaultCodes[run].append(currentFaultCode)

                    # Get interval for WorkSMs and drySMs
                    try:
                        category : str = row['Workflow']
                    except:
                        #old name for workflow state, needed if running script on past data
                        category : str = row['WorkSM']
                    try:
                        dryCategory : str = row['SmDrying']
                    except:
                        #also old name 
                        dryCategory : str = row['DrySM']

                    if (category != lastCategory and category != None):
                        if index != 0:
                            #marks index where last category ended
                            self.endsList[run][lastCategory] = index - 1
                        #marks where new category has started, one after last one ended 
                        self.startsList[run][category] = index
                        lastCategory = category
                    
                    if (dryCategory != lastDryCategory and dryCategory != None):
                        if index != 0:
                            self.endsDryList[run][lastDryCategory] = index - 1
                        self.startsDryList[run][dryCategory] = index
                        lastDryCategory = dryCategory
                    #check if a row is complete
                    fullRow = True
                    for key in currentRow:
                        fullRow = fullRow and currentRow[key] != None
                    # If a row is complete, iterate the index.
                    if fullRow:
                        
                        self.rows.append(currentRow)
                        index += 1
       
            
                for bat in range(run+1): #this will probably usually only be one iteration, as currently the script only works on one csv file at a time. Could be changed in the future Henry Synnott 10/26/23
                    #dictionary that will hold all of the summary info
                    addDict = {}
                    # Ensure that batch interval is fully set.
                    if constants.Column.batch.value not in self.endsList[bat]:
                        if len(self.startsList) < bat+2 or constants.Column.batch.value not in self.startsList[bat+1]:
                            self.endsList[bat][constants.Column.batch.value] = len(self.rows) - 1
                        else: # Backup in case detection earlier missed setting the end value and can be estimated closer using the next run's start.
                            self.endsList[bat][constants.Column.batch.value] = self.startsList[bat][constants.Column.batch.value + 1] - 1
                    
                    # Calculate values
                    errorCodes = ""
                    for code in uniqueNonzeroFaultCodes[bat]:
                        errorCodes += str(code) + ", "
                    addDict[constants.SummaryKey.batch_key.value] = self.rows[self.startsList[bat][constants.Column.batch.value]][constants.Column.batch.value]
                    addDict[constants.SummaryKey.fault_codes_key.value] = errorCodes[:-2]
                    addDict[constants.SummaryKey.batch_start_key.value] = self.rows[self.startsList[bat][constants.Column.batch.value]][constants.Column.timeStamp.value]
                    addDict[constants.SummaryKey.batch_end_key.value] = self.rows[self.endsList[bat][constants.Column.batch.value]][constants.Column.timeStamp.value]
                    
                    addDict[constants.SummaryKey.run_duration_key.value] = self.calculateRunDuration(bat)
                    addDict[constants.SummaryKey.drying_duration_key.value] = self.calculateDryingDuration(bat)
                    addDict[constants.SummaryKey.total_mass_processed_key.value] = self.calculateTotalMassProc(bat)
                    addDict[constants.SummaryKey.initial_mass_key.value] = self.calculateInitialPlasmaMass(bat)
                    addDict[constants.SummaryKey.enclosure_integrity_ending_pressure_key.value] = self.calculateEnclosureIntegrityEndingPressure(bat)
                    try: 
                        addDict[constants.SummaryKey.aerosol_integrity_ending_pressure_key.value] = self.calculateAerosolIntegrityEndingPressure(bat)
                    except:
                        addDict[constants.SummaryKey.aerosol_integrity_ending_pressure_key.value] = constants.Output.error.value

                    try:
                        addDict[constants.SummaryKey.pre_pdc_integrity_ending_pressure_key.value] = self.calculatePrePDCIntegrityEndingPressure(bat)
                    except:
                        addDict[constants.SummaryKey.pre_pdc_integrity_ending_pressure_key.value] = constants.Output.error.value

                    addDict[constants.SummaryKey.pre_prd_integrity_average_leak_rate_key.value] = self.calculatePrePDCIntegrityAverageLeakRate(bat)

                    initialExhaustTempSpikes = self.calculateInitialExhaustTempSpikes(bat)
                    addDict[constants.SummaryKey.initial_exhaust_temperature_spike_key6.value] = initialExhaustTempSpikes[0]
                    addDict[constants.SummaryKey.initial_exhaust_temperature_spike_key7.value] = initialExhaustTempSpikes[1]

                    dryingStatistics = self.calculateDryingStatistics(bat)
                    addDict[constants.SummaryKey.minimum_exhaust_temperature_key6.value] = dryingStatistics[0]
                    addDict[constants.SummaryKey.minimum_exhaust_temperature_key7.value] = dryingStatistics[1]
                    if addDict[constants.SummaryKey.total_mass_processed_key.value] == constants.Output.error.value or addDict[constants.SummaryKey.drying_duration_key.value] == constants.Output.error.value:
                        addDict[constants.SummaryKey.average_plasma_flow_rate_key.value] = constants.Output.error.value
                    else:    
                        addDict[constants.SummaryKey.average_plasma_flow_rate_key.value] = addDict[constants.SummaryKey.total_mass_processed_key.value] / addDict[constants.SummaryKey.drying_duration_key.value]
                    addDict[constants.SummaryKey.peak_plasma_flow_rate_key.value] = dryingStatistics[2]
                    addDict[constants.SummaryKey.minimum_plenum_pressute_key.value] = dryingStatistics[3]
                    addDict[constants.SummaryKey.peak_plenum_pressure_key.value] = dryingStatistics[4]
                    addDict[constants.SummaryKey.minimum_aerosol_pressure_key.value] = dryingStatistics[5]
                    addDict[constants.SummaryKey.peak_aerosol_pressure_key.value] = dryingStatistics[6]
                    addDict[constants.SummaryKey.minimum_drying_chamber_pressure_pt08_key.value] = dryingStatistics[7]
                    addDict[constants.SummaryKey.peak_drying_chamber_pressure_pt08_key.value] = dryingStatistics[8]
                    addDict[constants.SummaryKey.minimum_drying_chamber_pressure_pt09_key.value] = dryingStatistics[9]
                    addDict[constants.SummaryKey.peak_drying_chamber_pressure_pt09_key.value] = dryingStatistics[10]
                    

                    endingExhaustTempSpikes = self.calculateEndingExhaustTempSpikes(bat)
                    addDict[constants.SummaryKey.ending_exhaust_temperature_spike_tt06_key.value] = endingExhaustTempSpikes[0]
                    addDict[constants.SummaryKey.ending_exhaust_temperature_spike_tt07_key.value] = endingExhaustTempSpikes[1]
                    
                    addDict[constants.SummaryKey.post_pdc_integrity_ending_pressure_key.value] = self.calculatePostPDCIntegrityEndingPressure(bat)
                    addDict[constants.SummaryKey.post_pdc_integrity_average_leak_rate_key.value] = self.calculatePostPDCIntegrityAverageLeakRate(bat)

                    #Added min max average for PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02 and average DPT01a and DPT01b -Payton
                    addDict[constants.SummaryKey.pt01_trend_min_key.value] = dryingStatistics[11]
                    addDict[constants.SummaryKey.pt01_trend_max_key.value] = dryingStatistics[12]
                    addDict[constants.SummaryKey.pt01_trend_avg_key.value] = dryingStatistics[13]

                    addDict[constants.SummaryKey.pt02_trend_min_key.value] = dryingStatistics[14]
                    addDict[constants.SummaryKey.pt02_trend_max_key.value] = dryingStatistics[15]
                    addDict[constants.SummaryKey.pt02_trend_avg_key.value] = dryingStatistics[16]
                    
                    addDict[constants.SummaryKey.pt05_trend_min_key.value] = dryingStatistics[17]
                    addDict[constants.SummaryKey.pt05_trend_max_key.value] = dryingStatistics[18]
                    addDict[constants.SummaryKey.pt05_trend_avg_key.value] = dryingStatistics[19]

                    addDict[constants.SummaryKey.pt08_trend_min_key.value] = dryingStatistics[20]
                    addDict[constants.SummaryKey.pt08_trend_max_key.value] = dryingStatistics[21]
                    addDict[constants.SummaryKey.pt08_trend_avg_key.value] = dryingStatistics[22]

                    
                    addDict[constants.SummaryKey.tt04_min_key.value] = dryingStatistics[23]
                    addDict[constants.SummaryKey.tt04_max_key.value] = dryingStatistics[24]
                    addDict[constants.SummaryKey.tt04_avg_key.value] = dryingStatistics[25]

                    addDict[constants.SummaryKey.tt05_min_key.value] = dryingStatistics[26]
                    addDict[constants.SummaryKey.tt05_max_key.value] = dryingStatistics[27]
                    addDict[constants.SummaryKey.tt05_avg_key.value] = dryingStatistics[28]

                    addDict[constants.SummaryKey.tt06_min_key.value] = dryingStatistics[29]
                    addDict[constants.SummaryKey.tt06_max_key.value] = dryingStatistics[30]
                    addDict[constants.SummaryKey.tt06_avg_key.value] = dryingStatistics[31]

                    addDict[constants.SummaryKey.tt07_min_key.value] = dryingStatistics[32]
                    addDict[constants.SummaryKey.tt07_max_key.value] = dryingStatistics[33]
                    addDict[constants.SummaryKey.tt07_avg_key.value] = dryingStatistics[34]

                    addDict[constants.SummaryKey.mfc01_min_key.value] = dryingStatistics[35]
                    addDict[constants.SummaryKey.mfc01_max_key.value] = dryingStatistics[36]
                    addDict[constants.SummaryKey.mfc01_avg_key.value] = dryingStatistics[37]

                    addDict[constants.SummaryKey.mfc02_min_key.value] = dryingStatistics[38]
                    addDict[constants.SummaryKey.mfc02_max_key.value] = dryingStatistics[39]
                    addDict[constants.SummaryKey.mfc02_avg_key.value] = dryingStatistics[40]

                    addDict[constants.SummaryKey.dpt01a_avg_key.value] = dryingStatistics[41]
                    addDict[constants.SummaryKey.dpt01b_avg_key.value] = dryingStatistics[42]
                    #equalibrium section
                    equalibriumStatistics = self.calculateEqualibriumStatistics(bat)
                    addDict[constants.SummaryKey.tt04E_min_key.value] = equalibriumStatistics[0]
                    addDict[constants.SummaryKey.tt04E_max_key.value] = equalibriumStatistics[1]
                    addDict[constants.SummaryKey.tt04E_avg_key.value] = equalibriumStatistics[2]
                    addDict[constants.SummaryKey.tt04E_std_key.value] = equalibriumStatistics[3]

                    addDict[constants.SummaryKey.tt05E_min_key.value] = equalibriumStatistics[4]
                    addDict[constants.SummaryKey.tt05E_max_key.value] = equalibriumStatistics[5]
                    addDict[constants.SummaryKey.tt05E_avg_key.value] = equalibriumStatistics[6]
                    addDict[constants.SummaryKey.tt05E_std_key.value] = equalibriumStatistics[7]

                    addDict[constants.SummaryKey.tt06E_min_key.value] = equalibriumStatistics[8]
                    addDict[constants.SummaryKey.tt06E_max_key.value] = equalibriumStatistics[9]
                    addDict[constants.SummaryKey.tt06E_avg_key.value] = equalibriumStatistics[10]
                    addDict[constants.SummaryKey.tt06E_std_key.value] = equalibriumStatistics[11]

                    addDict[constants.SummaryKey.tt07E_min_key.value] = equalibriumStatistics[12]
                    addDict[constants.SummaryKey.tt07E_max_key.value] = equalibriumStatistics[13]
                    addDict[constants.SummaryKey.tt07E_avg_key.value] = equalibriumStatistics[14]
                    addDict[constants.SummaryKey.tt07E_std_key.value] = equalibriumStatistics[15]

                    addDict[constants.SummaryKey.mfc01E_min_key.value] = equalibriumStatistics[16]
                    addDict[constants.SummaryKey.mfc01E_max_key.value] = equalibriumStatistics[17]
                    addDict[constants.SummaryKey.mfc01E_avg_key.value] = equalibriumStatistics[18]
                    addDict[constants.SummaryKey.mfc01E_std_key.value] = equalibriumStatistics[19]

                    addDict[constants.SummaryKey.mfc02E_min_key.value] = equalibriumStatistics[20]
                    addDict[constants.SummaryKey.mfc02E_max_key.value] = equalibriumStatistics[21]
                    addDict[constants.SummaryKey.mfc02E_avg_key.value] = equalibriumStatistics[22]
                    addDict[constants.SummaryKey.mfc02E_std_key.value] = equalibriumStatistics[23]

                    addDict[constants.SummaryKey.dpt01aE_avg_key.value] = equalibriumStatistics[24]
                    addDict[constants.SummaryKey.dpt01aE_std_key.value] = equalibriumStatistics[25]
                    addDict[constants.SummaryKey.dpt01bE_avg_key.value] = equalibriumStatistics[26]
                    addDict[constants.SummaryKey.dpt01bE_std_key.value] = equalibriumStatistics[27]

                    chartCreator = charter.Charter(out_file_name, self.startsList[bat][constants.Column.batch.value], self.endsList[bat][constants.Column.batch.value],self.rows,addDict)
                    chartCreator.createCharts()
                    print("Data summation finished :)")
                

    def getDateTime(self, timeStamp : str):
        ''' Return the DateTime for the value of the string representation of the timestamp given.'''
        dateTimeFormat = "%Y-%m-%d %H:%M:%S" 
        dateTimeFormat2 = "%m/%d/%Y %H:%M" # Different log files had different formats sometimes even when looking the same in text editor
        try:
            return datetime.datetime.strptime(timeStamp, dateTimeFormat)
        except: # This could still throw value error.
            try: 
                return datetime.datetime.strptime(timeStamp, dateTimeFormat2)
            except:
                print("\nThere was an error trying to parse the timestamp. Assuming this timestamp isn't for a different run.\nGiven time: ")
                print(str(timeStamp) + "\nExpected format for date is " + dateTimeFormat + " or " + dateTimeFormat2)
                return None
    def areDifferentRuns(self, timeStamp : str, nextTimeStamp : str):
        ''' Return true if nextTimeStamp is more than a 2 minute different from timeStamp, indicating different runs in the log file. '''
        timeStampOffsetForNewBatchMinutes = 2 # Minimum number of minutes between two consecturive timestamps that indicates a new run.
        
        timeStampOffsetForNewBatchSeconds = timeStampOffsetForNewBatchMinutes * 60 # Minimum number of seconds between two consecturive timestamps that indicates a new run.
        time1 = self.getDateTime(timeStamp)
        time2 = self.getDateTime(nextTimeStamp)
        if time1 is None or time2 is None: # If timestamp error, assume it's still the same run.
            return False
        timeDifference = time2 - time1
        return timeDifference.days > 0 or timeDifference.seconds >= timeStampOffsetForNewBatchSeconds

    ######################## Summary functions #####################################
    def calculateRunDuration(self, batch):
        '''
        Calculate the run duration for the given batch in minutes.
        '''
        try:
            startInd = self.startsList[batch][constants.WorkState.runDeflectorDown.value]
            endInd = self.endsList[batch][constants.WorkState.unlockDoor.value]
            return self.calculateDuration(startInd, endInd)
        except:
            return constants.Output.error.value

    def calculateDryingDuration(self, batch):
        '''
        Calculate the time (in minutes) spent in the drying state
        '''
        try:
            startInd = self.startsDryList[batch][constants.DryState.drying.value]
            endInd = self.endsDryList[batch][constants.DryState.drying.value]
            return self.calculateDuration(startInd, endInd)
        except:
            return constants.Output.error.value
    def calculateTotalMassProc(self, batch):
        ''' Calculate the initial mass minus ending mass by averaging scale '''

        try:
            numSecondsToAverageSCALEOver = 5 # The number of seconds to average the value of SCALE over for the first segment of each interval for calculating the total mass processed.
            initialStart, initialEnd = 0,0
            try:
                initialStart = self.startsList[batch][constants.WorkState.runEnclosureCheck.value]
                initialEnd = self.endsList[batch][constants.WorkState.runEnclosureCheck.value]
            except:
                #CassetteCheck was an older value for EnclosureCheck, kept for backwards compatibility
                initialStart = self.startsList[batch][constants.WorkState.runCassetteCheck.value]
                initialEnd = self.endsList[batch][constants.WorkState.runCassetteCheck.value]

            initialEnd = self.getFirstSeconds(initialStart, initialEnd, numSecondsToAverageSCALEOver)
            endingStart = self.startsList[batch][constants.WorkState.runDeflectorUp.value] 
            endingEnd = self.endsList[batch][constants.WorkState.runDeflectorUp.value]
            initialAverage = self.average(initialStart, initialEnd, constants.Column.scale.value)
            endingAverage = self.average(endingStart, endingEnd, constants.Column.scale.value)
            return initialAverage - endingAverage
        except Exception as exc:
            print(exc.with_traceback)
            print(exc)
            return constants.Output.error.value

    def calculateInitialPlasmaMass(self, batch):
        ''' Calculate the initial mass averaging scale '''

        try:
            numSecondsToAverageSCALEOver = 5 # The number of seconds to average the value of SCALE over for the first segment of each interval for calculating the total mass processed.
            initialStart, initialEnd = 0,0
            try:
                initialStart = self.startsList[batch][constants.WorkState.runEnclosureCheck.value]
                initialEnd = self.endsList[batch][constants.WorkState.runEnclosureCheck.value]
            except:
                #CassetteCheck was an older value for EnclosureCheck, kept for backwards compatibility
                initialStart = self.startsList[batch][constants.WorkState.runCassetteCheck.value]
                initialEnd = self.endsList[batch][constants.WorkState.runCassetteCheck.value]
            initialEnd = self.getFirstSeconds(initialStart, initialEnd, numSecondsToAverageSCALEOver)
            initialAverage = self.average(initialStart, initialEnd, constants.Column.scale.value)
            return initialAverage
        except:
            return constants.Output.error.value
    def calculateEnclosureIntegrityEndingPressure(self, batch):
        ''' Calculate the enclosure integrity ending pressure for this batch by averaging PT07 for the last 2 seconds of Wrk_runCassettecheckSM with PV05=1'''
        try: 
            numEndSecondsAveragePT07 = 2 # The number of seconds at the end of the interval to average PT07 over.
            startInd, endInd = self.getIndicesWithPVConditions(self, batch, constants.WorkState.runCassetteCheck.value, True, False)
            return self.average(self.getLastSeconds(startInd, endInd, numEndSecondsAveragePT07), endInd, constants.Column.PT07.value)
        except Exception as exc:
            return constants.Output.error.value
    def calculateAerosolIntegrityEndingPressure(self, batch):
        ''' Calculate the aerosol integrity ending pressure by averaging PT03 for the last 2 seconds of Wrk_runAerosolCheckSM'''
        try: 
            numEndSecondsAveragePT03 = 2 # The number of seconds at the end of the interval to average PT03 over.
            startInd = self.startsList[batch][constants.WorkState.runAerosolCheck.value]
            endInd = self.endsList[batch][constants.WorkState.runAerosolCheck.value]
            return self.average(self.getLastSeconds(startInd, endInd, numEndSecondsAveragePT03), endInd, constants.Column.PT03.value)
        except:
            return constants.Output.error.value
    def calculatePrePDCIntegrityEndingPressure(self, batch):
        ''' Calculate the pre PDC integrity ending pressure for this batch by averaging PT06 for the last 2 seconds of Wrk_runDispPrecheckSM and PV05 and PV07 = 1'''
        try:
            numEndSecondsAveragePT06 = 2 # The number of seconds at the end of the interval to average PT06 over.
            startInd, endInd = self.getIndicesWithPVConditions(batch,constants.WorkState.runDispPrecheck.value, True, True)
            return self.average(self.getLastSeconds(startInd, endInd, numEndSecondsAveragePT06), endInd, constants.Column.PT06.value)
        except:
            return constants.Output.error.value
    def calculatePrePDCIntegrityAverageLeakRate(self, batch):
        ''' Calculate the pre PDC integrity average leak rate for this batch by averaging FS01 for the last 2 seconds of Wrk_runDispPrecheckSM and PV05 and PV07 = 1'''
        try:
            startInd, endInd = self.getIndicesWithPVConditions(batch, constants.WorkState.runDispPrecheck.value, True, True)
            return self.average(startInd, endInd, constants.Column.FS01.value)
        except:
            return constants.Output.error.value
    def calculateInitialExhaustTempSpikes(self, batch):
        '''
        Calculated the initial exhaust temperature spikes

        Return (in order) initial exhaust temperature spike for TT06 and TT07 when WorkSM = Wrk_runDryingSM & DrySM = Priming

        '''
        try: 
            startInd = self.startsList[batch][constants.WorkState.runDrying.value]
            endInd = self.endsList[batch][constants.WorkState.runDrying.value]
        except:
            return constants.Output.error.value, constants.Output.error.value
        #Initialize mins and maxes
        initialExhaustTempSpike6 = sys.float_info.min
        initialExhaustTempSpike7 = sys.float_info.min
        # Loop through drying state and get max and mins
        currentInd = startInd
        currentRow = self.rows[currentInd]
        while(currentInd <= endInd):
            currentRow = self.rows[currentInd]
            if currentRow[constants.Column.dryState.value] == constants.DryState.priming.value:
                initialExhaustTempSpike6 = max(initialExhaustTempSpike6, float(currentRow[constants.Column.TT06.value]))
                initialExhaustTempSpike7 = max(initialExhaustTempSpike7, float(currentRow[constants.Column.TT07.value]))
            currentInd += 1

        return initialExhaustTempSpike6, initialExhaustTempSpike7
    def calculateDryingStatistics(self, batch):
        '''
        Calculated the statistics that use the drying state as the condition.

        Return (in order) min exhaust temp (6,7),
        peak plasma flow rate, min plenum pressure,
        peak plenum pressure, min aero pressure, peak aero pressure, min dry chamber pressure (TT08),
        peak dry chamber pressure (TT08), min dry chamber pressure (TT09), peak dry chamber pressure (TT09),
        PT05 trend (min, max, avg), PT08 (min, max, avg)
        '''
        try:
            startDryInd = self.startsDryList[batch][constants.DryState.drying.value]
            endDryInd = self.endsDryList[batch][constants.DryState.drying.value]
            print ('Drying state starting index: ', startDryInd + 2, ' ending index: ', endDryInd + 2)
        except:
            return (constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value, 
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value, 
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value)
        
        #Calculate values that are just averaged across the duration
        # Added PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02, DPT01a, DPT01b - payton
        pt01TrendAvg = self.average(startDryInd, endDryInd, constants.Column.pt01Trend.value)
        pt02TrendAvg = self.average(startDryInd, endDryInd, constants.Column.pt02Trend.value)
        pt05TrendAvg = self.average(startDryInd, endDryInd, constants.Column.pt05Trend.value)
        pt08TrendAvg = self.average(startDryInd, endDryInd, constants.Column.pt08Trend.value)

        tt04Avg = self.average(startDryInd, endDryInd, constants.Column.TT04.value)
        tt05Avg = self.average(startDryInd, endDryInd, constants.Column.TT05.value)
        tt06Avg = self.average(startDryInd, endDryInd, constants.Column.TT06.value)
        tt07Avg = self.average(startDryInd, endDryInd, constants.Column.TT07.value)

        mfc01Avg = self.average(startDryInd, endDryInd, constants.Column.MFC01.value)
        mfc02Avg = self.average(startDryInd, endDryInd, constants.Column.MFC02.value)

        #dpt01a and dpt01b were physically removed from machine, kept in try just in case script is run on past data - Henry 
        try: 
            dpt01aAvg = self.average(startDryInd, endDryInd, constants.Column.DPT01a.value)
            dpt01bAvg = self.average(startDryInd, endDryInd, constants.Column.DPT01b.value)
        except:
            dpt01aAvg = constants.Output.error.value
            dpt01bAvg = constants.Output.error.value
        
        #Initialize mins and maxes
        #This section starts at the computer's minimum or maximmum, ensures you don't miss any numbers -Payton
        minExhaustTemp6 = sys.float_info.max
        minExhaustTemp7 = sys.float_info.max
        peakPlasmaFlowRate = sys.float_info.min
        peakPlasmaFlowRateEndSec = float(self.rows[endDryInd][constants.Column.time.value])
        numLastSecondsToIgnorePeakPlasma = 10
        # The number of seconds at the end of this interval to ignore for calculating the peak plasma flow rate
        peakPlasmaFlowRateTargetSec = peakPlasmaFlowRateEndSec - numLastSecondsToIgnorePeakPlasma
        minPlenumPressure = sys.float_info.max
        peakPlenumPressure = sys.float_info.min
        minAeroPressure = sys.float_info.max
        peakAeroPressure = sys.float_info.min
        minDryChamberPressure8 = sys.float_info.max
        peakDryChamberPressure8 = sys.float_info.min
        minDryChamberPressure9 = sys.float_info.max
        peakDryChamberPressure9 = sys.float_info.min

        #Added min max for PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02 - Payton
        pt01TrendMin = sys.float_info.max
        pt01TrendMax = sys.float_info.min
        
        pt02TrendMin = sys.float_info.max
        pt02TrendMax = sys.float_info.min
        
        pt05TrendMin = sys.float_info.max
        pt05TrendMax = sys.float_info.min
        
        pt08TrendMin = sys.float_info.max
        pt08TrendMax = sys.float_info.min

        tt04Min = sys.float_info.max
        tt04Max = sys.float_info.min

        tt05Min = sys.float_info.max
        tt05Max = sys.float_info.min

        tt06Min = sys.float_info.max
        tt06Max = sys.float_info.min

        tt07Min = sys.float_info.max
        tt07Max = sys.float_info.min

        mfc01Min = sys.float_info.max
        mfc01Max = sys.float_info.min
        
        mfc02Min = sys.float_info.max
        mfc02Max = sys.float_info.min
        # Loop through drying state and get max and mins
        currentInd = startDryInd
        currentRow = self.rows[currentInd]
        while(currentInd <= endDryInd):
            currentRow = self.rows[currentInd]
            
            minExhaustTemp6 = min(minExhaustTemp6, float(currentRow[constants.Column.TT06.value]))
            minExhaustTemp7 = min(minExhaustTemp7, float(currentRow[constants.Column.TT07.value]))
            peakPlasmaFlowRate = max(peakPlasmaFlowRate, float(currentRow[constants.Column.periPumpFlow.value])) if float(currentRow[constants.Column.time.value]) <= peakPlasmaFlowRateTargetSec else peakPlasmaFlowRate 
            minPlenumPressure = min(minPlenumPressure, float(currentRow[constants.Column.PT05.value]))
            peakPlenumPressure = max(peakPlenumPressure, float(currentRow[constants.Column.PT05.value]))
            try:
                minAeroPressure = min(minAeroPressure, float(currentRow[constants.Column.PT03.value]))
                peakAeroPressure = max(peakAeroPressure, float(currentRow[constants.Column.PT03.value]))
            except:
                minAeroPressure, peakAeroPressure = constants.Output.error.value, constants.Output.error.value
                
            minDryChamberPressure8 = min(minDryChamberPressure8, float(currentRow[constants.Column.PT08.value]))
            peakDryChamberPressure8 = max(peakDryChamberPressure8, float(currentRow[constants.Column.PT08.value]))
            minDryChamberPressure9 = min(minDryChamberPressure9, float(currentRow[constants.Column.PT09.value]))
            peakDryChamberPressure9 = max(peakDryChamberPressure9, float(currentRow[constants.Column.PT09.value]))
            #Added PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02 - Payton
            pt01TrendMin = min(pt01TrendMin, float(currentRow[constants.Column.pt01Trend.value]))
            pt01TrendMax = max(pt01TrendMax, float(currentRow[constants.Column.pt01Trend.value]))
            pt02TrendMin = min(pt02TrendMin, float(currentRow[constants.Column.pt02Trend.value]))
            pt02TrendMax = max(pt02TrendMax, float(currentRow[constants.Column.pt02Trend.value]))
            pt05TrendMin = min(pt05TrendMin, float(currentRow[constants.Column.pt05Trend.value]))
            pt05TrendMax = max(pt05TrendMax, float(currentRow[constants.Column.pt05Trend.value]))
            pt08TrendMin = min(pt08TrendMin, float(currentRow[constants.Column.pt08Trend.value]))
            pt08TrendMax = max(pt08TrendMax, float(currentRow[constants.Column.pt08Trend.value]))

            tt04Min = min(tt04Min, float(currentRow[constants.Column.TT04.value]))
            tt04Max = max(tt04Max, float(currentRow[constants.Column.TT04.value]))
            tt05Min = min(tt05Min, float(currentRow[constants.Column.TT05.value]))
            tt05Max = max(tt05Max, float(currentRow[constants.Column.TT05.value]))
            tt06Min = min(tt06Min, float(currentRow[constants.Column.TT06.value]))
            tt06Max = max(tt06Max, float(currentRow[constants.Column.TT06.value]))
            tt07Min = min(tt07Min, float(currentRow[constants.Column.TT07.value]))
            tt07Max = max(tt07Max, float(currentRow[constants.Column.TT07.value]))

            mfc01Min = min(mfc01Min, float(currentRow[constants.Column.MFC01.value]))
            mfc01Max = max(mfc01Max, float(currentRow[constants.Column.MFC01.value]))
            mfc02Min = min(mfc02Min, float(currentRow[constants.Column.MFC02.value]))
            mfc02Max = max(mfc02Max, float(currentRow[constants.Column.MFC02.value]))
            currentInd += 1

        return (minExhaustTemp6, minExhaustTemp7,
                peakPlasmaFlowRate,
                minPlenumPressure, peakPlenumPressure,
                minAeroPressure, peakAeroPressure,
                minDryChamberPressure8, peakDryChamberPressure8,
                minDryChamberPressure9, peakDryChamberPressure9,
                pt01TrendMin, pt01TrendMax, pt01TrendAvg,
                pt02TrendMin, pt02TrendMax, pt02TrendAvg,
                pt05TrendMin, pt05TrendMax, pt05TrendAvg,
                pt08TrendMin, pt08TrendMax, pt08TrendAvg,
                tt04Min, tt04Max, tt04Avg,
                tt05Min, tt05Max, tt05Avg,
                tt06Min, tt06Max, tt06Avg,
                tt07Min, tt07Max, tt07Avg,
                mfc01Min, mfc01Max, mfc01Avg,
                mfc02Min, mfc02Max, mfc02Avg,
                dpt01aAvg, dpt01bAvg)


    def calculateEqualibriumStatistics(self, batch):
        '''
        Calculated the statistics that use the drying state as the condition.

        Return (in order) min exhaust temp (6,7),
        peak plasma flow rate, min plenum pressure,
        peak plenum pressure, min aero pressure, peak aero pressure, min dry chamber pressure (TT08),
        peak dry chamber pressure (TT08), min dry chamber pressure (TT09), peak dry chamber pressure (TT09),
        PT05 trend (min, max, avg), PT08 (min, max, avg)
        '''
        try:
            startEquInd = self.startsDryList[batch][constants.DryState.drying.value] + 300 # starts around 65*
            endEquInd = self.endsDryList[batch][constants.DryState.drying.value] - 50 # ends before spike
            print ('Equilibrium state starting index: ', startEquInd + 2, ' ending index: ', endEquInd + 2)
            #Henry - some drying runs end in a error pretty quickly, and so the equilibrium stage using these hardcoded values was beginning before it started
            #so this checks if the equilibrium start/end indices are at valid points
            if startEquInd >= endEquInd:
                print("Drying stage too short to calculate equilibrium statistics (must be >350 data points)")
                raise Exception("Short drying stage")
        except:
            return (constants.Output.error.value, constants.Output.error.value, constants.Output.error.value, 
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value, 
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value, constants.Output.error.value, constants.Output.error.value,
            constants.Output.error.value)
        
        #Calculate values that are just averaged across the duration
        # Added PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02, DPT01a, DPT01b - payton
        pt01TrendAvg = self.average(startEquInd, endEquInd, constants.Column.pt01Trend.value)
        pt02TrendAvg = self.average(startEquInd, endEquInd, constants.Column.pt02Trend.value)
        pt05TrendAvg = self.average(startEquInd, endEquInd, constants.Column.pt05Trend.value)
        pt08TrendAvg = self.average(startEquInd, endEquInd, constants.Column.pt08Trend.value)

        tt04EAvg = self.average(startEquInd, endEquInd, constants.Column.TT04.value)
        tt05EAvg = self.average(startEquInd, endEquInd, constants.Column.TT05.value)
        tt06EAvg = self.average(startEquInd, endEquInd, constants.Column.TT06.value)
        tt07EAvg = self.average(startEquInd, endEquInd, constants.Column.TT07.value)

        mfc01EAvg = self.average(startEquInd, endEquInd, constants.Column.MFC01.value)
        mfc02EAvg = self.average(startEquInd, endEquInd, constants.Column.MFC02.value)
        
        try:
            dpt01aEAvg = self.average(startEquInd, endEquInd, constants.Column.DPT01a.value)
            dpt01bEAvg = self.average(startEquInd, endEquInd, constants.Column.DPT01b.value)
        except:
            dpt01aEAvg = constants.Output.error.value
            dpt01bEAvg = constants.Output.error.value

        tt04EStDev = self.stdev(startEquInd, endEquInd, constants.Column.TT04.value)
        tt05EStDev = self.stdev(startEquInd, endEquInd, constants.Column.TT05.value)
        tt06EStDev = self.stdev(startEquInd, endEquInd, constants.Column.TT06.value)
        tt07EStDev = self.stdev(startEquInd, endEquInd, constants.Column.TT07.value)

        mfc01EStDev = self.stdev(startEquInd, endEquInd, constants.Column.MFC01.value)
        mfc02EStDev = self.stdev(startEquInd, endEquInd, constants.Column.MFC02.value)
        try: 
            dpt01aEStDev = self.stdev(startEquInd, endEquInd, constants.Column.DPT01a.value)
            dpt01bEStDev = self.stdev(startEquInd, endEquInd, constants.Column.DPT01b.value)
        except:
            dpt01aEStDev = constants.Output.error.value
            dpt01bEStDev = constants.Output.error.value
        
        #Initialize mins and maxes
        #This section starts at the computer's minimum or maximmum, ensures you don't miss any numbers -Payton
        minExhaustTemp6 = sys.float_info.max
        minExhaustTemp7 = sys.float_info.max
        peakPlasmaFlowRate = sys.float_info.min
        peakPlasmaFlowRateEndSec = float(self.rows[endEquInd][constants.Column.time.value])
        numLastSecondsToIgnorePeakPlasma = 10
        # The number of seconds at the end of this interval to ignore for calculating the peak plasma flow rate
        peakPlasmaFlowRateTargetSec = peakPlasmaFlowRateEndSec - numLastSecondsToIgnorePeakPlasma
        minPlenumPressure = sys.float_info.max
        peakPlenumPressure = sys.float_info.min
        minAeroPressure = sys.float_info.max
        peakAeroPressure = sys.float_info.min
        minDryChamberPressure8 = sys.float_info.max
        peakDryChamberPressure8 = sys.float_info.min
        minDryChamberPressure9 = sys.float_info.max
        peakDryChamberPressure9 = sys.float_info.min

        #Added min max for PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02 - Payton
        pt01TrendMin = sys.float_info.max
        pt01TrendMax = sys.float_info.min
        pt02TrendMin = sys.float_info.max
        pt02TrendMax = sys.float_info.min
        pt05TrendMin = sys.float_info.max
        pt05TrendMax = sys.float_info.min
        pt08TrendMin = sys.float_info.max
        pt08TrendMax = sys.float_info.min

        tt04EMin = sys.float_info.max
        tt04EMax = sys.float_info.min
        tt05EMin = sys.float_info.max
        tt05EMax = sys.float_info.min
        tt06EMin = sys.float_info.max
        tt06EMax = sys.float_info.min
        tt07EMin = sys.float_info.max
        tt07EMax = sys.float_info.min

        mfc01EMin = sys.float_info.max
        mfc01EMax = sys.float_info.min
        mfc02EMin = sys.float_info.max
        mfc02EMax = sys.float_info.min

        # Loop through drying state and get max and mins
        currentInd = startEquInd
        currentRow = self.rows[currentInd]
        while(currentInd <= endEquInd):
            currentRow = self.rows[currentInd]
            
            minExhaustTemp6 = min(minExhaustTemp6, float(currentRow[constants.Column.TT06.value]))
            minExhaustTemp7 = min(minExhaustTemp7, float(currentRow[constants.Column.TT07.value]))
            peakPlasmaFlowRate = max(peakPlasmaFlowRate, float(currentRow[constants.Column.periPumpFlow.value])) if float(currentRow[constants.Column.time.value]) <= peakPlasmaFlowRateTargetSec else peakPlasmaFlowRate 
            minPlenumPressure = min(minPlenumPressure, float(currentRow[constants.Column.PT05.value]))
            peakPlenumPressure = max(peakPlenumPressure, float(currentRow[constants.Column.PT05.value]))
            try:
                minAeroPressure = min(minAeroPressure, float(currentRow[constants.Column.PT03.value]))
                peakAeroPressure = max(peakAeroPressure, float(currentRow[constants.Column.PT03.value]))
            except:
                minAeroPressure, peakAeroPressure = constants.Output.error.value, constants.Output.error.value

            minDryChamberPressure8 = min(minDryChamberPressure8, float(currentRow[constants.Column.PT08.value]))
            peakDryChamberPressure8 = max(peakDryChamberPressure8, float(currentRow[constants.Column.PT08.value]))
            minDryChamberPressure9 = min(minDryChamberPressure9, float(currentRow[constants.Column.PT09.value]))
            peakDryChamberPressure9 = max(peakDryChamberPressure9, float(currentRow[constants.Column.PT09.value]))
            #Added PT01, PT02, TT04, TT05, TT06, TT07, MFC01, MFC02 - Payton
            pt01TrendMin = min(pt01TrendMin, float(currentRow[constants.Column.pt01Trend.value]))
            pt01TrendMax = max(pt01TrendMax, float(currentRow[constants.Column.pt01Trend.value]))
            pt02TrendMin = min(pt02TrendMin, float(currentRow[constants.Column.pt02Trend.value]))
            pt02TrendMax = max(pt02TrendMax, float(currentRow[constants.Column.pt02Trend.value]))
            pt05TrendMin = min(pt05TrendMin, float(currentRow[constants.Column.pt05Trend.value]))
            pt05TrendMax = max(pt05TrendMax, float(currentRow[constants.Column.pt05Trend.value]))
            pt08TrendMin = min(pt08TrendMin, float(currentRow[constants.Column.pt08Trend.value]))
            pt08TrendMax = max(pt08TrendMax, float(currentRow[constants.Column.pt08Trend.value]))

            tt04EMin = min(tt04EMin, float(currentRow[constants.Column.TT04.value]))
            tt04EMax = max(tt04EMax, float(currentRow[constants.Column.TT04.value]))
            tt05EMin = min(tt05EMin, float(currentRow[constants.Column.TT05.value]))
            tt05EMax = max(tt05EMax, float(currentRow[constants.Column.TT05.value]))
            tt06EMin = min(tt06EMin, float(currentRow[constants.Column.TT06.value]))
            tt06EMax = max(tt06EMax, float(currentRow[constants.Column.TT06.value]))
            tt07EMin = min(tt07EMin, float(currentRow[constants.Column.TT07.value]))
            tt07EMax = max(tt07EMax, float(currentRow[constants.Column.TT07.value]))

            mfc01EMin = min(mfc01EMin, float(currentRow[constants.Column.MFC01.value]))
            mfc01EMax = max(mfc01EMax, float(currentRow[constants.Column.MFC01.value]))
            mfc02EMin = min(mfc02EMin, float(currentRow[constants.Column.MFC02.value]))
            mfc02EMax = max(mfc02EMax, float(currentRow[constants.Column.MFC02.value]))
            currentInd += 1

        return (tt04EMin, tt04EMax, tt04EAvg, tt04EStDev,
                tt05EMin, tt05EMax, tt05EAvg, tt05EStDev,
                tt06EMin, tt06EMax, tt06EAvg, tt06EStDev,
                tt07EMin, tt07EMax, tt07EAvg, tt07EStDev,
                mfc01EMin, mfc01EMax, mfc01EAvg, mfc01EStDev,
                mfc02EMin, mfc02EMax, mfc02EAvg, mfc02EStDev,
                dpt01aEAvg, dpt01aEStDev, dpt01bEAvg, dpt01bEStDev)

    def calculateEndingExhaustTempSpikes(self, batch):
        '''
        Calculated the ending exhaust temperature spikes

        Return (in order) ending exhaust temperature spike for TT06 and TT07 when DrySM = DryingGasContinue
        '''
        try: 
            startInd = self.startsDryList[batch][constants.DryState.dryingGasContinue.value]
            endInd = self.endsDryList[batch][constants.DryState.dryingGasContinue.value]
        except:
            return constants.Output.error.value, constants.Output.error.value
        #Initialize mins and maxes
        endingExhaustTempSpike6 = sys.float_info.min
        endingExhaustTempSpike7 = sys.float_info.min
        # Loop through drying state and get max and mins
        currentInd = startInd
        currentRow = self.rows[currentInd]
        while(currentInd <= endInd):
            currentRow = self.rows[currentInd]
            
            endingExhaustTempSpike6 = max(endingExhaustTempSpike6, float(currentRow[constants.Column.TT06.value]))
            endingExhaustTempSpike7 = max(endingExhaustTempSpike7, float(currentRow[constants.Column.TT07.value]))
            
            currentInd += 1

        return endingExhaustTempSpike6, endingExhaustTempSpike7

    def calculatePostPDCIntegrityEndingPressure(self, batch):
        ''' Calculate the pre PDC integrity ending pressure for this batch by averaging PT06 for the last 2 seconds of Wrk_runDispIntegritySM and PV05 and PV07 = 1'''

        try:
            numEndSecondsAveragePT06 = 2 # Number of seconds at the end of the following interval to average PT06 over. 
            startInd, endInd = self.getIndicesWithPVConditions(batch, constants.WorkState.runDispIntegrity.value, True, True)
            return self.average(self.getLastSeconds(startInd, endInd, numEndSecondsAveragePT06), endInd,constants.Column.PT06.value) 
        except:
            return constants.Output.error.value
    def calculatePostPDCIntegrityAverageLeakRate(self, batch):
        ''' Calculate the post PDC integrity average leak rate for this batch by averaging FS01 for the duration of Wrk_runDispIntegritySM and PV05 and PV07 = 1'''
        try:
            startInd, endInd = self.getIndicesWithPVConditions(batch, constants.WorkState.runDispIntegrity.value, True, True)
            return self.average(startInd, endInd,constants.Column.FS01.value)
        except:
            return constants.Output.error.value

            
    ##################### Helper Functions for Summary Statistics ###############################
    # Util Functions
    def calculateDuration(self, startInd, endInd):
        '''
        Calculate the duration between two indices in minutes.
        '''
        
        secondsPerMinute = 60 # Number of seconds in a minute to use for unit conversion.
        return (float(self.rows[endInd][constants.Column.time.value]) - float(self.rows[startInd][constants.Column.time.value])) / secondsPerMinute

    def getFirstSeconds(self, startInd, endInd, time):
        ''' Return the index for the last row within the given number of time seconds that is within endInd'''

        targetTime = float(self.rows[startInd][constants.Column.time.value]) + time
        currentInd = startInd
        currentRow = self.rows[currentInd]
        while (currentInd <= endInd and float(currentRow[constants.Column.time.value]) <= targetTime):
            currentInd += 1
            currentRow = self.rows[currentInd]
        # Return last row in interval or last row if at end of data
        return currentInd - 1
    def getLastSeconds(self, startInd, endInd, time):

        ''' Return the index for the first row within the given number of time seconds before endInd that matches state '''

        targetTime = float(self.rows[endInd][constants.Column.time.value]) - time
        currentInd = endInd
        currentRow = self.rows[currentInd]
        while (currentInd >= startInd and float(currentRow[constants.Column.time.value]) >= targetTime):
            currentInd -= 1
            currentRow = self.rows[currentInd]
        # Return earliest row in interval or first row if at start of data
        return currentInd + 1 

    def average(self, startInd, endInd, col):
        ''' Average the values for the column col between start and end (inclusive). col must be numeric column. '''

        asum = 0 # Sum being accumulated over the interval.
        count = 0 # Number of self.rows that the sum is being counted using.
        currentInd = startInd
        while(currentInd <= endInd):
            asum += float(self.rows[currentInd][col])
            count += 1
            currentInd += 1
        return asum / count if count > 0 else 0

    #function added by Payton
    def stdev(self, startInd, endInd, col):
        ''' Average the values for the column col between start and end (inclusive). col must be numeric column. '''
        
        stdlist = []
        
        currentInd = startInd
        while(currentInd <= endInd):
            stdlist += [float(self.rows[currentInd][col])]
            currentInd += 1
        return n.stdev(stdlist)


    def getIndicesWithPVConditions(self, batch : int, col : str, PV05 : bool, PV07 : bool):
        ''' Return (in order) the start and end indices for the interval of col in batch that checks if PV05 and PV07 = 1 if given as true (ignores them if given as false). '''

        startInd = self.startsList[batch][col]
        endInd = self.endsList[batch][col]
        currentInd = startInd
        currentRow = self.rows[currentInd]
        startReturn = startInd
        endReturn = endInd
        inInterval = False # True if currentInd is inside the interval matching the WorkSM state and whichever PV conditions are given.
        while (currentInd <= endInd):
            currentRow = self.rows[currentInd]
            match = self.matchPV05PV07(currentInd,PV05,PV07)
            if match and (not(inInterval)):
                startReturn = currentInd
                inInterval = True
            elif (not(match)) and inInterval:
                endReturn = currentInd - 1
                inInterval = False
                return startReturn, endReturn
            currentInd += 1
        return startReturn, endReturn

    def matchPV05PV07(self, rowInd : int, PV05 : bool, PV07 : bool):
        ''' Checks if PV05 and PV07 are set to 1 in digital out if given as true. They are ignored if given as false. '''
        bitPVStringCheck = '1' # The string for the PV bit that is checked for equality if PV05 or PV07 are given as true.
        try: 
            dout = self.rows[rowInd][constants.Column.digitalOut.value]
        except: 
            print("Column DigOut not found, attempting to find with past column names...")
            dout = self.rows[rowInd][constants.Column.digatlOut_oldName.value]
        return (not(PV05) or dout[5] == bitPVStringCheck) and (not(PV07) or dout[7] == bitPVStringCheck)



