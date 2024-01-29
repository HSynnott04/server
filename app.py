import shutil
import tempfile
from flask import Flask, render_template, request, redirect, send_file
from werkzeug.utils import secure_filename
import threading
import velLogScript
import os
import zipfile
import time
import random

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def threadedFileCleanup():
    print("trying to clean up file structure")
    if numberOfActiveRequests == 0:
        try:
            shutil.rmtree('datafiles')
            os.makedirs('datafiles/downloads')
            os.makedirs('datafiles/uploads')
        except:
            print('fatal error on automatically cleaning server tree, make sure file structure is intact')
            exit 
    time.sleep(3600)
    threadedFileCleanup()


#necessary variables
app = Flask(__name__)
ALLOWED_EXTENSIONS = {'csv'}
numberOfActiveRequests = 0
fileCleaner = threading.Thread(name="Cleaner", target=threadedFileCleanup)
fileCleaner.daemon = True
fileCleaner.start()


#initial cleanup on startup 
try:
    shutil.rmtree('datafiles')
    os.makedirs('datafiles/downloads')
    os.makedirs('datafiles/uploads')
except:
    print('fatal error on server startup cleaning server tree, make sure file structure is intact')
    exit 

#SITE ROUTES
@app.route("/")
def index():
    return render_template("index.html")

@app.route('/uploader', methods = ['POST'])
def upload_file():
    global numberOfActiveRequests
    randId = random.randint(1, 1000000000)
    if request.method == 'POST' and 'file' in request.files:
        f = request.files['file']
        if f.filename == '':
            return 'No selected file', 400
        if not allowed_file(f.filename):
            return 'File type not allowed', 400
    
        else:
            numberOfActiveRequests = numberOfActiveRequests + 1 
            os.mkdir(f"datafiles/uploads/{randId}")
            os.mkdir(f"datafiles/downloads/{randId}")
            try:
                f.save(f"datafiles/uploads/{randId}/{secure_filename(f.filename)}")
            except:
                numberOfActiveRequests = numberOfActiveRequests - 1 
                return "error saving file"
            
            try:
                v = velLogScript.velLogScript()
                filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)) + f"\\datafiles\\uploads\\{randId}", secure_filename(f.filename))
                v.main(filepath)
            except Exception as e:
                print(repr(e))
                numberOfActiveRequests = numberOfActiveRequests - 1
                return "error running analysis script on supplied file"
            
            try:
                os.remove(filepath)
                path = os.path.join(os.path.dirname(os.path.abspath(__file__)) + f"\\datafiles\\downloads\\{randId}", 'results.zip')

                zipf = zipfile.ZipFile(path,'w', zipfile.ZIP_DEFLATED)
                for root,dirs, files in os.walk(os.path.dirname(os.path.abspath(__file__)) + f"\\datafiles\\uploads\\{randId}"):
                    for file in files:
                        zipf.write(os.path.join(root,file), arcname=file)
                        os.remove(os.path.join(root, file))
                zipf.close()
            except:
                numberOfActiveRequests = numberOfActiveRequests - 1
                return "error writing zip file with results"

            
            cache = tempfile.NamedTemporaryFile()
            with open(path, 'rb') as fp:
                shutil.copyfileobj(fp, cache)
                cache.flush()
            cache.seek(0)
            os.remove(path)
            os.rmdir(f"datafiles/uploads/{randId}")
            os.rmdir(f"datafiles/downloads/{randId}")
            numberOfActiveRequests = numberOfActiveRequests - 1
            return send_file(cache, as_attachment=True, download_name='result.zip')
      
   