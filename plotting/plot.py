#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import arrow
import humanize
import numpy as np
import pandas
from bokeh.plotting import figure, curdoc
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, formatters, Spacer, Circle, OpenURL, TapTool
from bokeh.models.tools import HoverTool, CrosshairTool

import datetime
from datetime import timedelta

db_client = MongoClient()
collection = db_client['pastebin_scraper']['pastes']

to_timestamp = np.vectorize(lambda x: (x - datetime.datetime(1970, 1, 1)).total_seconds())
from_timestamp = np.vectorize(lambda x: datetime.datetime.fromtimestamp(x))

time_lim = arrow.utcnow().replace(hours=-12).datetime
data = pandas.DataFrame(list(collection.find({'date':{'$gt':time_lim}, }, {'_id':0, 'content':0, 'first_seen':0, 'last_seen':0, 'scrape_url':0}).sort('date', -1)))

exp = data['expire'] > '1970-01-01 01:00:00'
source_nonexp = ColumnDataSource(data[~exp])
source_exp = ColumnDataSource(data[exp])
source_nonexp.add([d.strftime("%d.%m.%Y %H:%M:%S") for d in data[~exp]['date']], name='datestring')
source_exp.add([d.strftime("%d.%m.%Y %H:%M:%S") for d in data[exp]['date']], name='datestring')
source_exp.add([humanize.naturaldelta(d) for d in data[exp]['expire']-data[exp]['date']], name='duration')

# Scatter plot
hover = HoverTool(tooltips=[('Date', '@datestring'), ('Title', '@title'), ('Key', '@key'), ('Syntax', '@syntax'), ('Size', '@size'), ('Duration', '@duration')])
# TODO: show duration and title only if available
crosshair = CrosshairTool(line_alpha=0.5)
tap = TapTool(callback=OpenURL(url='@full_url'), renderers=[])
# TODO: Instead of OpenURL, load and show paste content from the database directly!

fig = figure(
	width=1200, plot_height=500,
	toolbar_location='above',
	x_axis_type='datetime',
	y_axis_type='log',
	title='{} pastebins of the last 12 hours'.format(len(data)),
	tools=['pan', 'xwheel_zoom', 'reset', hover, crosshair, tap]
)
fig.xaxis.axis_label = 'UTC Time'
fig.yaxis.axis_label = 'Paste length'

default_style = dict(fill_alpha=0.7, size=3, line_color=None)
nonexp_sc = fig.scatter('date', 'size', source=source_nonexp, fill_color='green', legend='Not expiring pastes', **default_style)
exp_sc = fig.scatter('date', 'size', source=source_exp, fill_color='red', legend='Expiring pastes', **default_style)
# TODO: Plot all in one scatter but use the "tag" to distinguish

exp_sc.selection_glyph = Circle(fill_color='red', fill_alpha=0.7, size=10, line_color=None)
exp_sc.nonselection_glyph = Circle(fill_color='red', fill_alpha=0.1, size=3, line_color=None)
nonexp_sc.selection_glyph = Circle(fill_color='green', fill_alpha=0.7, size=10, line_color=None)
nonexp_sc.nonselection_glyph = Circle(fill_color='green', fill_alpha=0.1, size=3, line_color=None)

tf = formatters.DatetimeTickFormatter()
tf.seconds = '%H:%M:%S'; tf.minsec = '%H:%M:%S'; tf.minutes = '%H:%M:%S'
tf.hourmin = '%H:%M:%S'; tf.hours = '%H:%M'
tf.days = '%d.%m.%y'; tf.months = '%b'; tf.years = '%Y'
fig.xaxis[0].formatter = tf
fig.yaxis[0].formatter = formatters.PrintfTickFormatter(format='%d')
fig.legend.orientation = 'horizontal'
fig.legend.background_fill_alpha = 0.5


# Horizontal histogram
date_exp, date_nonexp = np.array(list(data[exp]['date'])), np.array(list(data[~exp]['date']))
hhist_exp, hedges_exp = np.histogram(to_timestamp(date_exp), bins=200)
hhist_nonexp, hedges_nonexp = np.histogram(to_timestamp(date_nonexp), bins=200)
hzeros_exp, hzeros_nonexp = np.zeros(len(hedges_exp)-1), np.zeros(len(hedges_nonexp)-1)
hmax_exp, hmax_nonexp = max(hhist_exp)*1.1, max(hhist_nonexp)*1.1

ph = figure(
	toolbar_location=None, plot_height=300, plot_width=fig.plot_width,
	y_range=(0,max(hmax_exp, hmax_nonexp)), x_range=fig.x_range, min_border=10, x_axis_type='datetime'
)
ph.xaxis[0].major_label_text_font_size = '0pt'
ph.xaxis[0].major_tick_line_color = None
ph.xaxis[0].ticker.num_minor_ticks = 0
ph.yaxis.axis_label = 'Pastes'

timestamps_exp, timestamps_nonexp = from_timestamp(hedges_exp), from_timestamp(hedges_nonexp)
ph.quad(left=timestamps_nonexp[:-1], bottom=0, top=hhist_nonexp, right=timestamps_nonexp[1:], color='green', fill_alpha=0.5)
ph.quad(left=timestamps_exp[:-1], bottom=0, top=hhist_exp, right=timestamps_exp[1:], color='red', fill_alpha=0.5)


# Vertical histogram
vhist_exp, vedges_exp = np.histogram(data[exp]['size'], bins=np.logspace(0, 7, 80))
vhist_nonexp, vedges_nonexp = np.histogram(data[~exp]['size'], bins=np.logspace(0, 7, 80))
vzeros_exp, vzeros_nonexp = np.zeros(len(vedges_exp)-1), np.zeros(len(vedges_nonexp)-1)
vmax_exp, vmax_nonexp = max(vhist_exp)*1.1, max(vhist_nonexp)*1.1

pv = figure(
	toolbar_location=None, plot_width=300, plot_height=fig.plot_height,
	x_range=(0, max(vmax_exp, vmax_nonexp)), y_range=fig.y_range, min_border=10,
	y_axis_type='log', y_axis_location='right'
)
pv.xaxis.axis_label = 'Pastes'
pv.yaxis[0].major_label_text_font_size = '0pt'
pv.yaxis[0].major_tick_line_color = None
pv.yaxis[0].ticker.num_minor_ticks = 0

pv.quad(left=0, bottom=vedges_nonexp[:-1], top=vedges_nonexp[1:], right=vhist_nonexp, color='green', fill_alpha=0.5)
pv.quad(left=0, bottom=vedges_exp[:-1], top=vedges_exp[1:], right=vhist_exp, color='red', fill_alpha=0.5)


layout = column(row(fig, pv), row(ph, Spacer(width=300, height=300)))

curdoc().add_root(layout)
curdoc().title = 'Pastebin Scraper'