import colorsys

from plotnine.ggplot import ggplot
from plotnine import labs, aes, \
        facet_grid, facet_wrap, \
        geom_line, geom_point, geom_bar, geom_errorbar, \
        theme, theme_light, theme_dark, theme_gray, element_text, element_line, element_rect, element_blank, guide_legend, guide_colorbar, \
        scale_x_log10, scale_y_log10, scale_x_continuous, scale_y_continuous, scale_y_discrete, scale_x_discrete, coord_cartesian, \
        scale_color_manual, \
        ylim, xlim



SIZEFACTOR = 0.75
(PLOT_W, PLOT_H) = (8.4*SIZEFACTOR, 4.8*SIZEFACTOR)  # inches

LINE_SIZE=0.8
POINT_SIZE=LINE_SIZE*2
WIDTH=0.7
ERRWIDTH=0.3

COLORS = [
"#c44601",
"#f57600",
"#8babf1",
"#0073e6",
"#054fb9",
]

COLORS_TC = {
        True: COLORS[0], False: COLORS[1]
        }

PLOTCOLORS = {
"fg"    : '#fff',
"fg0"   : '#fbf1c7',
"fg1"   : '#ebdbb2',
"fg3"   : '#bdae93',
"gray"  : '#928374',
"bg2"   : '#504945',
"bg1"   : '#3c3836',
"bg0"   : '#282828',
"bg"    : '#000',
"g1"    : '#333',
"g2"    : '#444',
"g3"    : '#555',
"g4"    : '#666',
"g5"    : '#777',
"g6"    : '#888',
"g7"    : '#999',
"g8"    : '#aaa',
"g9"    : '#bbb',
"g10"   : '#ccc',
"g11"   : '#ddd',
"g12"   : '#eee',
              }


def GLOBAL_THEME(smallw:bool=False,smallh:bool=False):
    t = theme(
                axis_text_x=element_text(angle=45),
                axis_ticks_major=element_line(color=PLOTCOLORS["bg2"]),
                #text = element_text(size = 18),
                strip_background=element_rect(color=PLOTCOLORS["bg2"], fill=PLOTCOLORS["g12"], size=0.6),
                strip_text=element_text(color=PLOTCOLORS["bg"]),
                panel_background=element_rect(fill=PLOTCOLORS["fg"]),
                #panel_grid_minor=element_line(color = "0.90", linetype="dotted", size=0.2),
                panel_grid_minor_x=element_blank() if smallw else element_line(color =PLOTCOLORS["g9"], linetype="dotted", size=0.15),
                panel_grid_minor_y=element_blank() if smallh else element_line(color =PLOTCOLORS["g9"], linetype="dotted", size=0.15),
                #panel_grid_minor=element_blank(),
                panel_grid_major=element_line(color =PLOTCOLORS["g7"], linetype="solid", size=0.3),
                #panel_ontop=element_rect(fill = "0.1", color="0.9",size=0.4),
                panel_ontop=None,
                # panel_border=element_line(color =PLOTCOLORS["bg2"],size=0.6), # WARN: broken in new version      # border around one subplot
                plot_background=element_rect(fill=None, color=None),

                #plot_title=element_text(color="#f000f0"),
                plot_title=element_blank(),
                legend_position="right",
                legend_key=element_rect(fill=PLOTCOLORS["fg"], color=PLOTCOLORS["g9"], size=0.25),
                #legend_key_size
                legend_margin=0,
                legend_background=element_rect(fill=None),
                legend_box_background=element_rect(fill=None),
                #subplots_adjust={"wspace":0.02,"hspace":0.25}, #make axis ticks visible?
                )
    return theme_light() + t




#                               ***
#                                ***
#                                 **
#                                 **
#                       ****      **       ****    ***  ****       ****
#            ****      * ***  *   **      * ***  *  **** **** *   * **** *
#           * ***  *  *   ****    **     *   ****    **   ****   **  ****
#          *   ****  **    **     **    **    **     **         ****
#         **         **    **     **    **    **     **           ***
#         **         **    **     **    **    **     **             ***
#         **         **    **     **    **    **     **               ***
#         **         **    **     **    **    **     **          ****  **
#         ***     *   ******      **     ******      ***        * **** *
#          *******     ****       *** *   ****        ***          ****
#           *****                  ***
#
#

def brighten(c,factor:float=6.0):
    h = c.lstrip('#')
    r,g,b = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
    h,s,v = colorsys.rgb_to_hsv(r,g,b)
    v *= factor
    v = int(v)
    v = min([v,255])
    v = max([v,0])
    # half effect of factor
    s *= 1/(1+((factor-1)/2))
    s = min([s,1.0])
    s = max([s,0])
    h = min([h,1.0])
    h = max([h,0])
    r,g,b = colorsys.hsv_to_rgb(h,s,v)
    return f"#{int(r):02x}{int(g):02x}{int(b):02x}"

def darken(c,factor:float=1.5):
    h = c.lstrip('#')
    r,g,b = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
    h,s,v = colorsys.rgb_to_hsv(r,g,b)
    v *= 1/factor
    v = int(v)
    v = min([v,255])
    v = max([v,0])
    # half effect of factor
    s *= 1/(1+((factor-1)/2))
    s = min([s,1.0])
    s = max([s,0])
    h = min([h,1.0])
    h = max([h,0])
    r,g,b = colorsys.hsv_to_rgb(h,s,v)
    return f"#{int(r):02x}{int(g):02x}{int(b):02x}"

