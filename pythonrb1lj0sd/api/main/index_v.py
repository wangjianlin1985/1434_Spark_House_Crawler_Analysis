# coding:utf-8
__author__ = "ila"

from flask import jsonify,send_from_directory
from . import main_bp
from utils.codes import *
# 管理员列表
@main_bp.route('/', methods=['GET'])
def index():
    msg = {'code': normal_code, 'message': 'python项目运行成功，请运行前台和后台页面程序'}
    return send_from_directory("templates/front","index.html")

@main_bp.route('/admin', methods=['GET'])
def admin():
    msg = {'code': normal_code, 'message': 'python项目运行成功，请运行前台和后台页面程序'}
    return send_from_directory("templates/front/admin/dist","index.html")

@main_bp.route('/css/<css>', methods=['GET'])
def admin_css(css):
    msg = {'code': normal_code, 'message': 'python项目运行成功，请运行前台和后台页面程序'}
    return send_from_directory("templates/front/admin/dist/css",css)

@main_bp.route('/js/<js>', methods=['GET'])
def admin_js(js):
    msg = {'code': normal_code, 'message': 'python项目运行成功，请运行前台和后台页面程序'}
    return send_from_directory("templates/front/admin/dist/js/",js)

@main_bp.route('/fonts/<fonts>', methods=['GET'])
def admin_fonts(fonts):
    msg = {'code': normal_code, 'message': 'python项目运行成功，请运行前台和后台页面程序'}
    return send_from_directory("templates/front/admin/dist/fonts/",fonts)

@main_bp.route('/img/<img>', methods=['GET'])
def admin_img(img):
    msg = {'code': normal_code, 'message': 'python项目运行成功，请运行前台和后台页面程序'}
    return send_from_directory("templates/front/admin/dist/img/",img)
