from urllib.request import urlopen
from io import BytesIO
import time

import tkinter as tk
from PIL import Image, ImageTk
import json

from rmq import RMQiface

urls = [
    'https://cdn.revjet.com/s3/csp/1578955925683/shine.png',
    'https://cdn.revjet.com/s3/csp/1578955925683/logo.svg',
    'https://tpc.googlesyndication.com/daca_images/simgad/13865403217536204307',
    'https://tpc.googlesyndication.com/daca_images/simgad/1948022358329940732?sqp=4sqPyQSWAUKTAQgAEhQNzczMPhUAAABAHQAAAAAlAAAAABgAIgoNAACAPxUAAIA_Kk8IWhABHQAAtEIgASgBMAY4A0CAwtcvSABQAFgAYFpwAngAgAEAiAEAkAEAnQEAAIA_oAEAqAEAsAGAreIEuAH___________8BxQEtsp0-MhoIvwMQ6gEYASABLQAAAD8wvwM46gFFAACAPw&rs=AOga4qmwNN2g28c_J8ehXFAoY4bOr7naGQ',
    'https://tpc.googlesyndication.com/simgad/12366423408132574325',
    'https://tpc.googlesyndication.com/simgad/3767484695346986263'
]


class HiddenRoot(tk.Tk):
    def __init__(self):
        tk.Tk.__init__(self)
        #hackish way, essentially makes root window
        #as small as possible but still "focused"
        #enabling us to use the binding on <esc>
        self.wm_geometry("0x0+0+0")

        self.window = MySlideShow(self)
        self.window.cycle()


class MySlideShow(tk.Toplevel):
    def __init__(self, *args, **kwargs):
        tk.Toplevel.__init__(self, *args, **kwargs)

        with open('reader_config.json', 'r') as f:
            config = json.load(f)

        host = config['host']
        usr = config['user']
        pwd = config['password']
        queue = config['filtered_images_queue_name']
        self.mq = RMQiface(host, queue, usr, pwd)
        self.img_error = Image.open('error.png')
        self.img_none = Image.open('none.png')

        #remove window decorations
        # self.overrideredirect(True)

        #save reference to photo so that garbage collection
        #does not clear image variable in show_image()
        self.persistent_image = None
        self.imageList = []
        self.pixNum = 0

        #used to display as background image
        self.label = tk.Label(self)
        self.label.pack(side="top", fill="both", expand=True)


    def cycle(self):
        while True:
            self.nexti()
            time.sleep(0.01)

    def nexti(self):
        # import random
        # url = random.choice(urls)
        url = self.mq.read()
        if url:
            try:
                img = Image.open(BytesIO(urlopen(url).read()))
                self.showImage(img)
                print(f'INFO:\tshowing {url}')
            except Exception:
                print(f'ERROR:\tnot a valid image: {url}')
        else:
            print('INFO:\tQueue is empty')
            time.sleep(1.0)



    def showImage(self, image):
        img_w, img_h = image.size
        scr_w, scr_h = self.winfo_screenwidth(), self.winfo_screenheight()
        width, height = min(scr_w, img_w), min(scr_h, img_h)
        image.thumbnail((width, height), Image.ANTIALIAS)

        #set window size after scaling the original image up/down to fit screen
        #removes the border on the image
        scaled_w, scaled_h = image.size
        self.wm_geometry("{}x{}+{}+{}".format(scaled_w,scaled_h,0,0))

        # create new image
        self.persistent_image = ImageTk.PhotoImage(image)
        self.label.configure(image=self.persistent_image)
        self.update()


slideShow = HiddenRoot()
# slideShow.window.attributes('-fullscreen', True)
# slideShow.window.attributes('-topmost', True)
slideShow.bind_all("<Escape>", lambda e: slideShow.destroy())
# slideShow.bind_all("<Return>", lambda e: slideShow.window.nexti()) # exit on esc
slideShow.update()
slideShow.mainloop()
