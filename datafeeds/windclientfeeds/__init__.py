# -*- coding:utf-8 -*-
# @Time    : 2019/12/2810:55
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : __init__.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is base wind client class and can used directly

import abc
import copy
from WindPy import w


class BaseWindClient(metaclass=abc.ABCMeta):

    def __init__(self):
        self.__waitTime = 120

    def connect(self):
        wait_time = copy.deepcopy(self.__waitTime)
        w.start(waitTime=wait_time)
        if not w.isconnected():
            raise BaseException("[BaseWindClient] wind client did't connect now, please try again later")
        return w
        


