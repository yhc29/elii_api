import datetime
import math

class Timer:
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.time = datetime.datetime.now()
        self.total_time = 0.0
        self.prev_time = 0.0
        self.progress = 0
        self.remaining_time = 0

    def click(self):
        new_time = datetime.datetime.now()
        running_time = (new_time - self.time).total_seconds()
        self.prev_time = running_time
        self.time = new_time
        return s_to_str(running_time)
    
    def stop(self):
        new_time = datetime.datetime.now()
        self.total_time = (new_time - self.start_time).total_seconds()
        return s_to_str(self.total_time)
    
    def get_progress(self,finished_count,total_count,p=10,show=False):
        progress_new = int(100*finished_count/total_count)
        result = None
        if progress_new != self.progress and progress_new%p == 0:
            self.progress = progress_new
            new_time = datetime.datetime.now()
            self.total_time = (new_time - self.start_time).total_seconds()
            self.remaining_time = round((self.total_time/self.progress)*(100-self.progress),2)
            result = str(self.progress)+"%" + " finished, " + s_to_str(self.remaining_time) + " remaining"
            print(result) if show else None
        return result
    
def s_to_str(seconds):
    hours = int(seconds//3600)
    seconds = seconds - 3600*hours
    minutes = int(seconds//60)
    seconds = round(seconds - 60*minutes,4)
    if hours>0:
        result = str(hours) + "h " + str(minutes) + "m "+str(seconds) + "s"
    elif minutes>0:
        result = str(minutes) + "m "+str(seconds) + "s"
    else:
        result = str(seconds) + "s"
    return result

