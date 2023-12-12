from app.log import logger
from app.schemas import MediaServerLibrary
from app.modules.plex import Plex
from plexapi.myplex import MyPlexAccount


class PlexMusic(Plex):
    """API for Plex
        继承根项目, 实现音乐库相关的api
    """

    def __init__(self):
        Plex.__init__(self)
        self.music_libraries = []
        self.music_playlists = []
        self.music_names = []

    def get_user_name(self):
        account = MyPlexAccount(self._token)
        return account.username

    def get_music_library(self):
        """
        获取媒体服务器所有音乐类型媒体库列表
        """
        if not self._plex:
            return []
        try:
            _libraries = self._plex.library.sections()
        except Exception as err:
            logger.error(f"Plex获取媒体服务器所有媒体库列表出错：{str(err)}")
            return []
        for library in _libraries:
            match library.type:
                case "artist":
                    library_type = '音乐'
                case _:
                    continue
            self.music_names.append(library.title)
            self.music_libraries.append(
                MediaServerLibrary(
                    id=library.key,
                    name=library.title,
                    path=library.locations,
                    type=library_type
                )
            )
        return self.music_libraries

    def get_playlists(self):
        """获取播放列表"""
        playlists = self._plex.playlists()
        for playlist in playlists:
            if playlist.isAudio:
                # music_playlists.append(
                #     {
                #         "name": playlist.title,
                #         "id": playlist.ratingKey,
                #         "url": playlist.thumbUrl,
                #     }
                # )
                self.music_playlists.append(playlist.title)
        return self.music_playlists

    def get_tracks_by_playlist(self, playlist_title):
        """获取播放列表的详细歌单"""
        if playlist_title not in self.music_playlists:
            logger.warn(f"Plex媒体库中播放列表为:{self.music_playlists}\n 不存在: {playlist_title}, 稍后会自动创建，如果失败请手动创建")
            return []
        playlist = self._plex.playlist(playlist_title)
        # 获取歌单中的歌曲
        tracks = playlist.items()
        music_names = [i.title for i in tracks]
        return music_names

    def create_playlist(self, title, items):
        """创建播放列表"""
        self._plex.createPlaylist(title=title, items=items, libtype='track')

    def set_tracks_to_playlist(self, playlist_title, tracks):
        """添加歌曲到歌单中"""
        playlist = self._plex.playlist(playlist_title)
        playlist.addItems(tracks)

    def search_music(self, name_singer, exact_match=True):
        """通过歌曲名在库中进行搜索"""
        name = name_singer[0]
        singer = name_singer[1]
        if len(self.music_libraries) == 1:
            search_res = self._plex.search(name, mediatype='track', sectionId=int(self.music_libraries[0].id))
        else:
            search_res = self._plex.search(name, mediatype='track')
        if len(search_res) > 1:
            if exact_match:
                search_res = [i for i in search_res if singer in i.grandparentTitle][:1]
            else:
                search_res = search_res[:1]
        return search_res


if __name__ == '__main__':
    pm = PlexMusic()
    ml = pm.get_user_name()
    pm.get_playlists()
    pm.get_tracks_by_playlist('经典华语')
    res = pm.search_music('七里香')
