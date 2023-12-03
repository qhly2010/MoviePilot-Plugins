from typing import List, Optional, Union, Dict, Generator, Tuple

from app.log import logger
from app.schemas import MediaType, MediaServerLibrary
from app.modules.emby import Emby
from app.utils.http import RequestUtils



class EmbyMusic(Emby):
    """API for Plex
        继承根项目, 实现音乐库相关的api
    """

    def __init__(self):
        Emby.__init__(self)
        self.music_libraries = []
        self.music_playlists = []
        self.music_names = []

    def __get_emby_librarys(self) -> List[dict]:
        """
        获取Emby媒体库列表
        """
        if not self._host or not self._apikey:
            return []
        req_url = f"{self._host}emby/Users/{self.user}/Views?api_key={self._apikey}"
        try:
            res = RequestUtils().get_res(req_url)
            if res:
                return res.json().get("Items")
            else:
                logger.error(f"User/Views 未获取到返回数据")
                return []
        except Exception as e:
            logger.error(f"连接User/Views 出错：" + str(e))
            return []

    def get_music_library(self):
        """
        获取媒体服务器所有音乐类型媒体库列表以及所有playlist
        """
        if not self._host or not self._apikey:
            return []
        emby_librarys = self.__get_emby_librarys() or []
        for library in emby_librarys:
            if library.get("CollectionType") == 'music':
                self.music_libraries.append(
                    MediaServerLibrary(
                        server="emby",
                        id=library.get("Id"),
                        name=library.get("Name"),
                        path=library.get("Path"),
                        type=library.get("CollectionType")
                    )
                )
            elif library.get("CollectionType") == 'playlists':
                url = f'{self._host}emby/Users/{self.user}/Items?ParentId={library.get("Id")}&api_key={self._apikey}'
                try:
                    res = self.get_data(url)
                    playlist = res.json().get("Items")
                except Exception as err:
                    logger.error(f"Emby获取播放列表失败: {err}")
                    playlist = []
                self.music_playlists += playlist
            else:
                continue
        logger.info(f"Emby中的播放列表为: {[i.get('Name') for i in self.music_playlists]}")
        return self.music_libraries

    def get_tracks_by_playlist(self, playlist_title):
        """获取播放列表的详细歌单"""
        playlist_id = None
        for i in self.music_playlists:
            if playlist_title == i.get('Name'):
                playlist_id = i.get('Id')
                break
        if not playlist_id:
            logger.warn(f"Emby媒体库中播放列表为:{self.music_playlists}\n 不存在: {playlist_title}, 稍后会自动创建，如果失败请手动创建")
            return '', []
        url = f'{self._host}emby/Users/{self.user}/Items?ParentId={playlist_id}&api_key={self._apikey}'
        try:
            res = self.get_data(url)
            tracks = res.json().get("Items")
        except Exception as err:
            logger.error(f"Emby获取播放列表失败: {err}")
            tracks = []
        music_ids = [i.get('Id') for i in tracks if i.get('Type') == 'Audio']
        return playlist_id, music_ids

    def create_playlist(self, name, ids):
        """创建播放列表"""
        if name in [i.get('Name') for i in self.music_playlists]:
            logger.info(f"歌单: {name} 已经存在，跳过创建")
        # Playlists
        url = f'{self._host}emby/Playlists?api_key={self._apikey}&userId={self.user}&Name={name}&Ids={ids}'
        try:
            res = self.post_data(
                url,
                headers={"Content-Type": "application/json"}
            )
            if res.status_code == 200:
                info = res.json()
                logger.info(f"Emby创建歌单成功:{info}")
                return True
            else:
                logger.error(f"Emby创建歌单失败:{name}")
                return False
        except Exception as e:
            logger.error(f"Emby创建歌单失败:{name}: {e}")
            return False

    def set_tracks_to_playlist(self, playlist_id, ids):
        """添加歌曲到歌单中"""
        url = f'{self._host}emby/Playlists/{playlist_id}/Items?api_key={self._apikey}&userId={self.user}&Ids={ids}'
        try:
            res = self.post_data(
                url,
                headers={"Content-Type": "application/json"}
            )
            if res.status_code == 200:
                info = res.json()
                logger.info(f"Emby歌单添加歌曲成功:{info}")
                return True
            else:
                logger.error(f"Emby歌单添加歌曲失败")
                return False
        except Exception as e:
            logger.error(e)
            return False

    def search_music(self, name):
        """通过歌曲名在库中进行搜索"""
        # /emby/Users/{}/Items?Recursive=true&SearchTerm={}&api_key={}
        url = f'{self._host}emby/Users/{self.user}/Items?Recursive=true&SearchTerm={name}&api_key={self._apikey}'
        try:
            res = self.get_data(url)
            count = res.json().get("TotalRecordCount")
            if count > 0:
                items = res.json().get("Items")
                ids_list = [i.get('Id') for i in items if i.get('Type') == 'Audio'][:1]
            else:
                ids_list = []
        except Exception as err:
            logger.error(err)
            return []
        return ids_list

    def mul_search_music(self, name_list):
        all_list = []
        for name in name_list:
            ids_list = self.search_music(name)
            all_list += ids_list
        return all_list


if __name__ == '__main__':
    em = EmbyMusic()
    em.get_music_library()
    media_playlist = "许嵩专版"
    t_tracks = ['断桥残雪', '有何不可']

    tracks = em.mul_search_music(t_tracks)
    playlist_id, music_ids = em.get_tracks_by_playlist(media_playlist)
    if playlist_id:
        ids = [i for i in tracks if i not in music_ids]
        em.set_tracks_to_playlist(playlist_id, ','.join(ids))
    else:
        em.create_playlist(media_playlist, ','.join(tracks))
    print(music_ids)


