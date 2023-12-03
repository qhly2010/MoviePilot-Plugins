import json
import os
import pytz
from datetime import datetime, timedelta
from threading import Event
from typing import List, Tuple, Dict, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple
from app.log import logger
from app.plugins.syncmusiclist.QQmusic import QQMusicApi
from app.plugins.syncmusiclist.cloudmusic import CloudMusic
from app.plugins.syncmusiclist.emby_music import EmbyMusic
from app.plugins.syncmusiclist.plex_music import PlexMusic


class SyncMusicList(_PluginBase):
    # 插件名称
    plugin_name = "同步QQ/网易云歌单"
    # 插件描述
    plugin_desc = "同步QQ/网易云歌单到plex&emby。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/baozaodetudou/MoviePilot-Plugins/main/icons/music.png"
    # 插件版本
    plugin_version = "1.7"
    # 插件作者
    plugin_author = "逗猫"
    # 作者主页
    author_url = "https://github.com/baozaodetudou"
    # 插件配置项ID前缀
    plugin_config_prefix = "music_"
    # 加载顺序
    plugin_order = 17
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    # 启动
    _enabled = False
    # 运行一次
    _onlyonce = False
    # 定时
    _cron = None
    # 媒体服务器
    _media_server = []
    # 网易云登录设置, 同步自己歌单需要登录，不是自己不需要登录
    _wylogin_type = 'phone'
    _wylogin_user = ''
    _wylogin_password = ''

    # 同步列表
    _wymusic_paths = ""
    _qqmusic_paths = ""
    # 退出事件
    _event = Event()

    def init_plugin(self, config: dict = None):
        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._media_server = config.get("media_server") or []
            self._wylogin_user = config.get("wylogin_password") or ""
            self._wylogin_password = config.get("wylogin_password") or ""
            self._wymusic_paths = config.get("wymusic_paths") or ""
            self._qqmusic_paths = config.get("qqmusic_paths") or ""

        # 停止现有任务
        self.stop_service()

        # 启动定时任务 & 立即运行一次
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._cron:
                logger.info(f"歌单同步服务启动，周期：{self._cron}")
                try:
                    self._scheduler.add_job(func=self.__run_sync_paylist,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="歌单同步")
                except Exception as e:
                    logger.error(f"歌单同步服务启动失败，原因：{str(e)}")
                    self.systemmessage.put(f"歌单同步服务启动失败，原因：{str(e)}")
            else:
                logger.info(f"歌单同步服务启动，周期：每2天")
                self._scheduler.add_job(func=self.__run_sync_paylist,
                                        trigger=CronTrigger.from_crontab("0 0 */2 * *"),
                                        name="歌单同步")
            if self._onlyonce:
                logger.info(f"歌单同步服务，立即运行一次")
                self._scheduler.add_job(func=self.__run_sync_paylist, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="歌单同步")
                # 关闭一次性开关
                self._onlyonce = False
                self.update_config({
                    "onlyonce": False,
                    "enabled": self._enabled,
                    "cron": self._cron,
                    "media_server": self._media_server,
                    "wylogin_user": self._wylogin_user,
                    "wylogin_password": self._wylogin_password,
                    "wymusic_paths": self._wymusic_paths,
                    "qqmusic_paths": self._qqmusic_paths
                })
            if self._scheduler.get_jobs():
                # 启动服务
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'chips': True,
                                            'multiple': True,
                                            'model': 'media_server',
                                            'label': '媒体服务器',
                                            'items': [
                                                {'title': 'Plex', 'value': 'plex'},
                                                {'title': 'Emby', 'value': 'emby'},
                                            ]
                                        }
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空自动'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'qqmusic_paths',
                                            'label': 'QQ音乐歌单同步设置',
                                            'rows': 4,
                                            'placeholder': '一行一个歌单配置留空不启用, 格式：QQ音乐歌单id:plex/emby播放列表名称 \n'
                                                           'eg: 2362260213: 经典歌曲。\n'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'wylogin_user',
                                            'label': '网易云手机号',
                                            'placeholder': '',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'wylogin_password',
                                            'label': '网易云密码',
                                            'placeholder': '',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'wymusic_paths',
                                            'label': '网易云歌单同步设置',
                                            'rows': 4,
                                            'placeholder': '一行一个歌单配置留空不启用, 格式：网易云音乐歌单id:plex/emby播放列表名称 \n'
                                                           'eg: 402924168: 经典歌曲。\n'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'title': '使用说明:',
                                            'text':
                                                '1. plex/emby服务器中存在音乐类型的库; \n'
                                                '2. plex/emby的播放列表需要提前创建好并且里边至少有一首歌曲; \n'
                                                '3. 如不存在会自动创建歌单, 如库中没符合的歌曲会创建失败; \n'
                                                '4. 歌单同步只会搜索已存在歌曲进行添加,不会自动下载; \n'
                                                '5. 歌曲匹配是模糊匹配只匹配曲名不匹配歌手; \n'
                                                '6. 网易云同步自己的歌单需要登录其他情况非强制登录; \n'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "cron": "0 0 */7 * *",
            "mode": "",
            "scraper_paths": "",
            "err_hosts": ""
        }

    def get_page(self) -> List[dict]:
        pass

    def __run_sync_paylist(self):
        """
        开始同步歌单
        """
        global pm, em
        if not self._wymusic_paths and not self._qqmusic_paths:
            logger.info("同步配置为空,不进行处理。告退......")
            return
        if not self._media_server:
            logger.info("没有可用的媒体服务器,不进行处理。告退......")
            return
        if 'plex' in self._media_server:
            # plex 初始化
            pm = PlexMusic()
            pm.get_music_library()
            pm.get_playlists()
        if 'emby' in self._media_server:
            # emby 初始化
            em = EmbyMusic()
            em.get_music_library()
        # 获取同步列表信息
        if self._qqmusic_paths:
            qq = QQMusicApi()
            qqmusic_paths = self._qqmusic_paths.split("\n")
            for path in qqmusic_paths:
                qq_play_id, media_playlist = path.split(':')
                logger.debug(f"QQ歌单id: {qq_play_id}, 媒体库播放列表名称: {media_playlist}")
                if qq_play_id and media_playlist:
                    qq_tracks = qq.get_playlist_by_id(qq_play_id)
                    logger.debug(f"QQ歌单 {qq_play_id} 获取歌曲: {qq_tracks}")
                    if 'plex' in self._media_server:
                        self.__t_plex(pm, qq_tracks, media_playlist)
                    if 'emby' in self._media_server:
                        self.__t_emby(em, qq_tracks, media_playlist)
                else:
                    logger.warn(f"QQ音乐歌单同步设置配置不规范,请认真检查修改")
        if self._wymusic_paths:
            cm = CloudMusic()
            if self._wylogin_user and self._wylogin_password:
                cm.login(self._wylogin_user, self._wylogin_password, phone=True)
            wymusic_paths = self._wymusic_paths.split("\n")
            for path in wymusic_paths:
                wy_play_id, media_playlist = path.split(':')
                logger.debug(f"网易云歌单id: {wy_play_id}, 媒体库播放列表名称: {media_playlist}")
                if wy_play_id and media_playlist:
                    wy_tracks = cm.songofplaylist(wy_play_id)
                    logger.debug(f"网易云歌单 {wy_play_id} 获取歌曲: {wy_tracks}")
                    if 'plex' in self._media_server:
                        self.__t_plex(pm, wy_tracks, media_playlist)
                    if 'emby' in self._media_server:
                        self.__t_emby(em, wy_tracks, media_playlist)
                else:
                    logger.warn(f"网易云音乐歌单同步设置配置不规范,请认真检查修改")
        return

    def __t_emby(self, em, t_tracks, media_playlist):
        logger.info("Emby开始同步歌单,涉及搜索时间较长请耐心等待.......")
        tracks = em.mul_search_music(t_tracks)
        playlist_id, music_ids = em.get_tracks_by_playlist(media_playlist)
        if playlist_id:
            ids = [i for i in tracks if i not in music_ids]
            em.set_tracks_to_playlist(playlist_id, ','.join(ids))
        else:
            em.create_playlist(media_playlist, ','.join(tracks))
        logger.info("Emby同步歌单完成,感谢耐心等待.......")
        return

    def __t_plex(self, pm, t_tracks, media_playlist):
        logger.info("Plex开始同步歌单,涉及搜索时间较长请耐心等待.......")
        add_tracks = []
        plex_tracks = pm.get_tracks_by_playlist(media_playlist)
        logger.debug(f"plex播放列表 {media_playlist} 已存在歌曲: {plex_tracks}")
        # 查找获取tracks
        for t_track in t_tracks:
            if t_track in plex_tracks:
                continue
            tracks = pm.search_music(t_track)
            add_tracks += tracks
        # 去重
        add_tracks = list(set(add_tracks))
        # 有歌曲写入没有就跳过
        if len(add_tracks) > 0:
            if len(plex_tracks) < 1:
                try:
                    # 创建如果存在创建失败就进行添加
                    pm.create_playlist(media_playlist, add_tracks)
                    logger.info(f"Plex创建播放列表[{media_playlist}]成功，并添加曲目: {[i.title for i in add_tracks]}")
                except Exception as err:
                    logger.error(f"{err}")
            else:
                try:
                    pm.set_tracks_to_playlist(media_playlist, add_tracks)
                    logger.info(f"Plex向播放列表[{media_playlist}]添加曲目 {[i.title for i in add_tracks]} 成功")
                except Exception as e:
                    logger.error(f"{e}")
        else:
            logger.info(f"Plex该歌单在媒体库搜索获取为空，创建/添加播放列表失败")
        logger.info("Plex同步歌单完成,感谢耐心等待.......")
        return

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))





