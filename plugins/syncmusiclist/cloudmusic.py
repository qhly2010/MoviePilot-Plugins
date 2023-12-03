### 一个利用wireshark抓包出来的pc客户端api制作的下载网易云音乐的音乐和Mv的一个小脚本。
### 修改https://github.com/xzap/NeteasyMusic/blob/master/neteasymusic.py
import time

import requests
import hashlib
import base64

from app.log import logger


class CloudMusic(object):
    def __init__(self):
        self.req = requests.Session()
        self.headers = {"Referer": "http://music.163.com/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.138 Safari/537.36",
                        }
        self.cookies = {"appver": "1.5.2",
                        "os": "pc",
                        "channel": "netease",
                        "osver": "Microsoft-Windows-7-Professional-Service-Pack-1-build-7601-64bit",
                        }
        self.req.cookies.update(self.cookies)
        self.req.headers.update(self.headers)

    def login(self, username, password, phone=False):
        """
        登陆到网易云音乐
        """
        action = 'http://music.163.com/api/login/'
        phone_action = 'http://music.163.com/api/login/cellphone/'
        password = password.encode('utf-8')
        data = {
            'username': username,
            'password': hashlib.md5(password).hexdigest(),
            'rememberLogin': 'true'
        }
        phone_data = {
            'phone': username,
            'password': hashlib.md5(password).hexdigest(),
            'rememberLogin': 'true'
        }
        try:
            if phone is True:
                r = self.req.post(phone_action, data=phone_data)
                self.cookies = r.cookies
                self.req.cookies.update(self.cookies)
                return r.json()
            else:
                r = self.req.post(action, data=data)
                print(r.cookies)
                self.cookies = r.cookies
                self.req.cookies.update(self.cookies)
                return r.json()
        except Exception as e:
            print(str(e))
            return {'code': 408}

    def encrypted_id(self, id):
        """
        为下载mp3转换id
        """
        byte1 = bytearray('3go8&$8*3*3h0k(2)2', "utf8")
        byte2 = bytearray(str(id), "utf8")
        byte1_len = len(byte1)
        for i in range(len(byte2)):
            byte2[i] = byte2[i] ^ byte1[i % byte1_len]
        m = hashlib.md5()
        m.update(byte2)
        result = m.digest()
        result = base64.encodebytes(result).decode()[:-1]
        result = result.replace('/', '_')
        result = result.replace('+', '-')
        return result

    def song(self, uid):
        """
        单曲信息
        Full request URI: http://music.163.com/api/song/detail/?id=28377211&ids=%5B28377211%5D
        GET  http://music.163.com/api/song/detail/  
        必要参数：   
            id：歌曲ID 
            ids：不知道干什么用的，用[]括起来的歌曲ID
        """
        url = "http://music.163.com/api/song/detail/"
        params = {"id": uid, "ids": "[%s]" % uid}
        r = self.req.get(url, params=params)
        return r.json()

    def playlist(self, uid):
        """
        歌单
        Full request URI: http://music.163.com/api/playlist/detail?id=37880978&updateTime=-1
        GET http://music.163.com/api/playlist/detail
        必要参数：
            id：歌单ID
        """
        url = "http://music.163.com/api/playlist/detail"
        params = {"id": uid}
        r = self.req.get(url, params=params)
        return r.json()
    
    def songofplaylist(self, uid):
        """
        获取歌单中歌曲
        """
        tracks = []
        # 循环进行重试
        # 设置最大重试次数
        max_retry_times = 5
        # 当前重试次数
        retry_times = 0
        while retry_times < max_retry_times:
            try:
                playlist = self.playlist(uid)['result']
                tracks = playlist['tracks']
                break  # 如果成功执行，跳出循环
            except Exception as e:
                logger.warn(f"第 {retry_times + 1} 次重试失败：获取歌单错误")
                retry_times += 1
                time.sleep(2)  # 添加延时
        track_names = [i.get('name').split(' (')[0].split('(')[0].split('（')[0] for i in tracks]
        return track_names

    def lyric(self, uid, os="pc", lv=-1, kv=-1):
        """
        歌词
        Full request URI: http://music.163.com/api/song/lyric?os=pc&id=93920&lv=-1&kv=-1&tv=-1
        GET http://music.163.com/api/song/lyric
        必要参数：
            id：歌曲ID
            lv：值为-1，我猜测应该是判断是否搜索lyric格式
            kv：值为-1，这个值貌似并不影响结果，意义不明
            tv：值为-1，是否搜索tlyric格式
        """

        url = "http://music.163.com/api/song/lyric"
        params = {"id": uid, "os": os, "kv": kv, "lv": lv}
        r = self.req.get(url, params=params)
        return r.json()['lrc']['lyric']

    def user_playlist(self, uid, offset=0, limit=100):
        """
        用户歌单
        http://music.163.com/api/user/playlist/?offset=0&limit=100&uid=uid
        """
        url = "http://music.163.com/api/user/playlist/"
        params = {"uid": uid, "offset": offset, "limit": limit}
        r = self.req.get(url, params=params)
        return r.json()

    def get_track(self, tracks, fname):
        """
        获取多轨列表中的歌曲信息
        """
        result = []
        for track in tracks:
            name = track['name'].strip().replace(" ", "_")
            album = track['album']['name'].strip().replace(" ", "_")
            uuid = track['id']
            artists = track['artists']
            singer = "_".join([artist['name'].strip().replace(" ", "_") for artist in artists]).replace(" ", "_")
            try:
                sid = track['hMusic']['dfsId']
                ext = track['hMusic']['extension']
            except:
                sid = track['mMusic']['dfsId']
                ext = track['mMusic']['extension']
            filename = "%s-%s.%s" % (name, singer, ext)
            link = 'http://m1.music.126.net/{}/{}.{}'.format(self.encrypted_id(sid), sid, ext)
            result.append((uuid, fname, filename, link, True))
        return result


if __name__ == '__main__':
    cm = CloudMusic()
    res = cm.songofplaylist("3136179094")
    print(res)

