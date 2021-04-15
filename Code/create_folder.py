# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 09:21:30 2021

@author: Jiaming.Zhou
"""
import os
folder_list = ["BOT_AOI", "TOP_AOI", "VI", "OST", "ICT", "PACK_AOI"]

def create_folder(week,folder_list=folder_list):
    
    if not os.path.isdir(week):
        os.makedirs(f"./{week}/{week}")
        for folder in folder_list:
            os.makedirs(f"./{week}/{week}/{folder}/Fpy/Daily")
        os.makedirs(f"./{week}/{week}/Customer/Defects")
        os.makedirs(f"./{week}/{week}/Customer/Fpy")

    else:
        os.chdir(f"./{week}")
        if not os.path.isdir(week):
            os.mkdir(f"./{week}")
            os.chdir(f"./{week}")
            for folder in folder_list:
                os.makedirs(f"./{folder}/Fpy/Daily")
            os.makedirs("./Customer/Defects")
            os.makedirs(f"./Customer/Fpy")
        else:
            os.chdir(f"./{week}")
            if not os.path.isdir("Customer"):
                os.makedirs("./Customer/Defects")
                os.makedirs(f"./Customer/Fpy")
                
            for folder in folder_list:
                folder = 'BOT_AOI'
                if not os.path.isdir(folder):
                    os.makedirs(f"./{folder}/Fpy/Daily")
                else:
                    os.chdir(f"./{folder}")
                    if not os.path.isdir("Fpy"):
                        os.makedirs("./Fpy/Daily")
                    else:
                        os.chdir("./Fpy")
                        if not os.path.isdir("Daily"):
                            os.mkdir("./Daily")
            
    return


