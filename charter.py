import constants
# import xlsxwriter module  
import xlsxwriter 
import datetime 
import math

class Charter:
    ''' Class to keep track of data values to track and then plot them when data is accumulated '''
    logSheetName = "LogSheet" # Filename of sheet containing log data for the run being charted.
    timeSheetName = "Elapsed Time" # Name of hidden time sheet that calculates elapsed time.
    filename = "" # Filename of workbook
    start = 0 # Start row index of entire log file for this run
    end = 0 # Last row index of entire log file for this run
    startRow = 1 # First row of logsheet to start reading for charts
    endRow = 0 # Last row of logsheet to read for creating charts
    rows = [] # Rows of data for charts to reference
    summaryInfo = {} # Set of summary information for this run
    columnNums = {}
    def __init__(self, filename, start, end, rows, summaryInfo):
        ''' Initialize an object for creating charts in the file with the specified filename based on rows starting at start and ending at end '''
        print(f"initializing charter for {filename}")
        self.filename = filename
        self.start = start
        self.end = end
        self.endRow = end - start
        self.rows = rows
        self.summaryInfo = summaryInfo
        self.columnNums = { # column number to use when referencing data for charts
                #These values don't matter anymore, they get updated right after the log sheet is created in createCharts method to reflect the key - Henry Synnott
                constants.Column.time : 0,
                constants.Column.elapsedTime : 0,
                constants.Column.MFC01 : 0,
                constants.Column.MFC02 : 0,
                constants.Column.scale : 0,
                constants.Column.PT03 : 0,
                constants.Column.PT05 : 0,
                constants.Column.PT06 : 0,
                constants.Column.PT07 : 0,
                constants.Column.PT08 : 0,
                constants.Column.PT09 : 0,
                constants.Column.TT05 : 0,
                constants.Column.TT06 : 0,
                constants.Column.FS01 : 0,
                constants.Column.periPumpFlow : 0,
                constants.Column.MFC01_P : 00,
                constants.Column.PT10 : 00
            }
        
    def createCharts(self):
        ''' Add the Full Run, Pressure, Plasma Delivery, and Perturbations charts to the given workbook '''
        workbook = xlsxwriter.Workbook(self.filename)
        # TODO: Maybe separate worksheet and chartsheet creation
        # Summary sheet
        # Get the list of keys and round values
        keyList = []
        summaryValues = []
        # Summary values that should be rounded to 3 decimal values instead of 2. 
        # TODO: Come up with a better solution if there's a larger range of possible decimal places to round to
        keysToRoundTo3 = [constants.SummaryKey.pt05_trend_min_key.value, constants.SummaryKey.pt05_trend_max_key.value, 
        constants.SummaryKey.pt05_trend_avg_key.value, constants.SummaryKey.pt08_trend_min_key.value, 
        constants.SummaryKey.pt08_trend_max_key.value, constants.SummaryKey.pt08_trend_avg_key.value]
        for key in self.summaryInfo:
            keyList.append(key) 
            if isinstance(self.summaryInfo[key],float):
                if key in keysToRoundTo3:
                    self.summaryInfo[key] = round(self.summaryInfo[key],3)
                else:
                    self.summaryInfo[key] = round(self.summaryInfo[key],2)
            summaryValues.append(self.summaryInfo[key])
        summarySheetName = "Summary"
        summarySheet = workbook.add_worksheet(summarySheetName)
        summarySheet.write_row(0,0,keyList)
        summarySheet.write_row(1,0,summaryValues)

        # Log sheet
        headers = []
        for key in self.rows[0]:
            headers.append(key)
            if key == constants.Column.time.value: # Add elapsed time header to the right of timesec
                headers.append(constants.Column.elapsedTime.value)
        worksheet = workbook.add_worksheet(self.logSheetName)
        worksheet.write_row(0,0, headers)

        # updates column indexes using key and column headers list created earlier in this method - Henry 
        #keys to remove list is later used to make sure data isn't being charted for keys that have been removed
        keysToRemove = []
        for key in self.columnNums:
            try:
                self.columnNums[key] = headers.index(key.value) 
            except:
                keysToRemove.append(key)
        
        for key in keysToRemove:       
            print("Values not found in data for key: " + key.value + ". This might be antiquated sensor value")                 
            self.columnNums.pop(key)
        

        elapsedTimes = []
        firstTime = self.rows[self.start][constants.Column.time.value]
        secondsPerMinute = 60 # Number of seconds in a minute to get time elapsed as minutes.
        for currentInd in range((self.end-self.start) + 1):
            values = []
            currentRow = self.rows[self.start + currentInd]
            currentTime = (currentRow[constants.Column.time.value])
            elapsedTime = "Error" # Placeholder in case there is an error calculating the elapsed time. TODO: Check how this affects charts
            if currentTime != "":
                try:
                    elapsedTime = ((currentRow[constants.Column.time.value] - firstTime) / secondsPerMinute)
                    elapsedTimes.append(elapsedTime)
                except: print(currentTime)
            for key in currentRow:
                values.append(currentRow[key])
                if key == constants.Column.time.value: # Add elapsed time to the right of timesec
                    values.append(elapsedTime)

            worksheet.write_row(currentInd+1,0,values)

        xLabel = "Elapsed Time (min)" # Label for the x-axis of the charts
        
        # Full run sheet
        # Columns whose axis is on the left of the full run chart.
        fullSheetName = "Full Run"
        fullChartTitle = "Detail Data"
        fullYLabel = 'Weight (g) and Flow Rate (L/min)'
        fullY2Label = 'Pressure (psig), Temperature (' + u'\N{DEGREE SIGN}' + 'C), and Flow Rate (mL/min)'
        leftLabelColumns = [constants.Column.MFC01,
            constants.Column.MFC02,
            constants.Column.scale]
        fullRightColumns = []
        for key in self.columnNums:
            if key != constants.Column.time:
                if key not in leftLabelColumns:
                    fullRightColumns.append(key)
        
         
        # Pressure Chart
        pressureSheetName = "Pressure"
        pressureChartTitle = "Pressure vs. Time"
        pressureYLabel = "Pressure (psig)"
        pressureColumns = [
            constants.Column.PT05,
            constants.Column.PT03,
            constants.Column.PT06,
            constants.Column.PT07,
            constants.Column.PT08,
            constants.Column.PT09,
            constants.Column.PT10,
            constants.Column.MFC01_P]
        
        # Plasma Chart
        plasmaSheetName = "Plasma Delivery"
        plasmaChartTitle = "Plasma Delivery vs. Time"
        plasmaYLabel = "Weight (g)"
        plasmaY2Label = "Flow Rate (mL/min)"
        plasmaLeftColumns = ([constants.Column.scale])
        plasmaRightColumns = ([constants.Column.periPumpFlow])

        # Perturbations Chart
        perturbationsSheetName = "Perturbations"
        perturbationsChartTitle = perturbationsSheetName + " vs. Time"
        perturbationsYLabel = "Pressure (psig)"
        perturbationsY2Label = "Flow Rate (mL/min)"
        perturbationsLeftColumns = [constants.Column.PT03, constants.Column.PT10, constants.Column.MFC01_P]

        perturbationsRightColumns = [constants.Column.periPumpFlow]

        allColumnLabels = [leftLabelColumns, fullRightColumns, pressureColumns, plasmaLeftColumns, plasmaRightColumns, perturbationsLeftColumns, perturbationsRightColumns]

        for key in keysToRemove:
            for l in allColumnLabels:
                try:
                    l.remove(key)
                except:
                    pass

        #full run
        self.addChartSheet(workbook, elapsedTimes, fullSheetName,fullChartTitle,leftLabelColumns,fullYLabel,xLabel,fullY2Label,fullRightColumns)

        #pressure chart
        self.addChartSheet(workbook, elapsedTimes, pressureSheetName,pressureChartTitle,pressureColumns,pressureYLabel,xLabel)
        
        #plasma chart
        self.addChartSheet(workbook, elapsedTimes, plasmaSheetName,plasmaChartTitle,plasmaLeftColumns,plasmaYLabel,xLabel,plasmaY2Label,plasmaRightColumns)

        #pertubation chart 
        self.addChartSheet(workbook, elapsedTimes, perturbationsSheetName,perturbationsChartTitle,perturbationsLeftColumns,perturbationsYLabel,xLabel,perturbationsY2Label,perturbationsRightColumns)

        print("Data added, saving to excel file. This might take a moment...")
        workbook.close()
        print("Data saved.")


    def addSeries(self,chartSheet,columnNum,isRight):
        ''' Add a data line using the data in column columnNum to chartSheet that references the secondary y axis if isRight. '''
        if(isRight):
            chartSheet.add_series({
                'name' : [self.logSheetName, 0, columnNum],
                'values': [self.logSheetName, self.startRow, columnNum, self.endRow, columnNum],
                'categories': [self.logSheetName, 1,self.columnNums[constants.Column.elapsedTime] ,self.endRow,self.columnNums[constants.Column.elapsedTime]],
                'y2_axis' : 1,
            })
        else:
            chartSheet.add_series({
                'name' : [self.logSheetName, 0, columnNum],
                'values': [self.logSheetName, self.startRow, columnNum, self.endRow, columnNum],
                'categories': [self.logSheetName, 1,self.columnNums[constants.Column.elapsedTime] ,self.endRow,self.columnNums[constants.Column.elapsedTime]],
            })
    def addChartSheet(self, book : xlsxwriter.Workbook, elapsedTimes, sheetName,chartTitle,leftColumns,ylabel,xlabel,y2label="", rightColumns=[]):
        ''' 
        Add a chart with the given labels and data references to the given book.

        Add a chart to the given book that is based against the list of elapsedTimes at intervals of 5 
        minutes with given names for the sheet, chart title, and axes and given column numbers based on 
        the primary y axis and optionally columns based on the secondary axis labeled with y2Label using 
        column numbers from keys in leftColumns. 
        '''
        #if antiquated data has been removed then there is the possibility of a chart with no data-in that case this will not generate a chart sheet
        if(leftColumns):
            chartSheet = book.add_chartsheet(sheetName)
            chart = book.add_chart({'type': 'scatter', 'subtype': 'straight'})
            for column in leftColumns:
                self.addSeries(chart,self.columnNums[column], False)
            for column in rightColumns:
                self.addSeries(chart,self.columnNums[column], True)
            chart.set_title({'name': chartTitle})
            chart.set_y_axis({'name': ylabel, 'min': 0})
            chart.set_x_axis({'name': xlabel, 'label_position': 'low', 'major_unit' : 5, 'min': 0, 'max' : math.ceil(elapsedTimes[-1])})
            if len(rightColumns) > 0:
                chart.set_y2_axis({'name':y2label})
            chart.set_legend({'position': 'bottom'})
            chartSheet.set_chart(chart)
        else:
            print(chartTitle + " did not have any data, chart sheet not added.")
    
