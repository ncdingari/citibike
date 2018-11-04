import numpy as np
import pandas as pd
import matplotlib.cm
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.lines as mlines
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import matplotlib.cbook as cbook

colors = ['blue','green','red']
lw_default = 1

alpha_default = 0.5
alphamultiplier = 1

years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
days = mdates.DayLocator()  # every day
yearsFmt = mdates.DateFormatter('%Y')
monthsFmt = mdates.DateFormatter('%m-%Y')
daysFmt = mdates.DateFormatter('%d')
figsize_w = 12
figsize_h = 6

figurepath = "//users/sajudson/Dropbox/WPI/DS504/HW3/figures/"

defaultfiguresaveformat = ".svg"

def lplotter(x,y1,y2,t1,t2,xlabel,y1label,y2label, filename, figurepath = figurepath, figsaveformat = defaultfiguresaveformat):

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(figsize_w, figsize_h))

    def ytickformat(x):
        return '$%1.2f' % x

    # round to nearest month...
    datemin = np.datetime64(x[0], 'M')
    datemax = np.datetime64(x[-1], 'M') + np.timedelta64(1, 'M')

    # plot y1
    axes[0].plot(x,y1,linewidth =lw_default, color = colors[0], alpha = alpha_default)
    axes[0].set_title(t1)

    # plot y2
    axes[1].plot(x,y2,linewidth=lw_default ,color = colors[1], alpha = alpha_default)

    axes[1].set_title(t2)

    axes[0].set_ylabel(y1label)
    axes[1].set_ylabel(y2label)
    axes[0].set_ylim(0, np.max(y1)*1.05)
    axes[1].set_ylim(0, np.max(y2)*1.05)

    # x and y labesls
    for ax in axes:
        ax.set_xlabel(xlabel)
        ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        ax.format_ydata = ytickformat
        ax.grid(True)
        # format the ticks
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        ax.set_xlim(datemin, datemax)

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    #save figure as PNG
    figfilename = figurepath+filename+figsaveformat
    plt.savefig(figfilename, bbox_inches='tight', dpi = (300))

    plt.show()
    return()


def lplotter2(x1,x2,y1,y2,t1,xlabel,ylabel, filename, plottype = 'scatter'):

    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(6, 6))

    def ytickformat(x):
        return '$%1.2f' % x

    # round to nearest month...
    datemin = np.datetime64(x1[0], 'M')
    datemax = np.datetime64(x1[-1], 'M') + np.timedelta64(1, 'M')

    if plottype == 'line':
        # plot y1
        ax.plot(x1,y1,linewidth =lw_default, color = colors[0], alpha = alpha_default*alphamultiplier, label="Subscribers")
        ax.plot(x2,y2,linewidth=lw_default*.5 ,color = colors[1], alpha = alpha_default, label = "Customers")
    else:
        ax.scatter(x1,y1,linewidth =lw_default, color = colors[0], alpha = alpha_default*alphamultiplier, label="Subscribers")
        ax.scatter(x2,y2,linewidth=lw_default*.5 ,color = colors[1], alpha = alpha_default, label = "Customers")

    ax.set_title(t1)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, max(np.max(y1),np.max(y2))*1.05)

    ax.set_xlabel(xlabel)
    ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    ax.format_ydata = ytickformat
    ax.grid(True)
    # format the ticks
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    ax.xaxis.set_minor_locator(months)
    ax.set_xlim(datemin, datemax)
    ax.legend()

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    #save figure as PNG
    figfilename = figurepath+filename+ defaultfiguresaveformat
    plt.savefig(figfilename, bbox_inches='tight', dpi = (300))

    plt.show()
    return()

def lplotter0(x1,y1,t1,xlabel,ylabel, filename, plottype = 'scatter'):

    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(6, 6))

    def ytickformat(x):
        return '$%1.2f' % x

    # round to nearest month...
    datemin = np.datetime64(x1[0], 'M')
    datemax = np.datetime64(x1[-1], 'M') + np.timedelta64(1, 'M')

    # plot y1

    if plottype == "line":
        ax.plot(x1,y1,linewidth =lw_default, color = colors[0], alpha = alpha_default*alphamultiplier, label="Subscribers")
    else:
        ax.scatter(x1,y1,linewidth =lw_default, color = colors[0], alpha = alpha_default*alphamultiplier, label="Subscribers")

    ax.set_title(t1)

    ax.set_ylabel(ylabel)
    ax.set_ylim(0, np.max(y1)*1.05)

    ax.set_xlabel(xlabel)
    ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    ax.format_ydata = ytickformat
    ax.grid(True)
    # format the ticks
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    ax.xaxis.set_minor_locator(months)
    ax.set_xlim(datemin, datemax)
    ax.legend()

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    #save figure as PNG
    figfilename = figurepath+filename+ defaultfiguresaveformat
    plt.savefig(figfilename, bbox_inches='tight', dpi = (300))

    plt.show()
    return()

def scatter(x1,y1,t1,xlabel,ylabel, filename, plottype = 'scatter'):

    alpha_default = 0.4

    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(6, 6))

    def ytickformat(x):
        return '$%1.2f' % x

    def xtickformat(x):
        return '$%1.2f' % x


    ax.scatter(x1,y1,linewidth =lw_default, color = colors[0], alpha = alpha_default*alphamultiplier, label="Subscribers")

    ax.set_title(t1)

    ax.set_ylabel(ylabel)
    ax.set_ylim(0, np.max(y1)*1.05)
    ax.set_xlim(0, np.max(x1)*1.05)

    ax.set_xlabel(xlabel)
    ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    ax.format_ydata = ytickformat
    ax.grid(True)
    # format the ticks
    ax.legend()

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    #save figure as PNG
    figfilename = figurepath+filename+ defaultfiguresaveformat
    plt.savefig(figfilename, bbox_inches='tight', dpi = (300))

    plt.show()
    return()

def plotChartList(df,filepath,plot_function,chartlist):
    for c in chartlist:
        plot_function(df.index, df[c['y1']], df[c['y2']], c['title1'], c['title2'],
                      c['xlabel'], c['y1label'], c['y2label'], filepath+c['filename'])
    return
