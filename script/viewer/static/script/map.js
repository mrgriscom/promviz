
REFRESH_INTERVAL = 300;

function init($div, data) {
    var map = L.map($div.attr('id')).setView([30, 0], 2);

    var mapboxLayer = function(tag) {
        return L.tileLayer('http://api.tiles.mapbox.com/v3/' + tag + '/{z}/{x}/{y}.png');
    };

    var layers = {
        'Oilslick': L.tileLayer('http://s3.amazonaws.com/oilslick/{z}/{x}/{y}.jpg', {maxZoom: 11}),
        'NGI': L.tileLayer('http://htonl.dev.openstreetmap.org/ngi-tiles/tiles/50k/{z}/{x}/{y}.png', {tms: true, maxZoom: 15}),
        'Classroom': L.tileLayer('http://maps-for-free.com/layer/relief/z{z}/row{y}/{z}_{x}-{y}.jpg', {maxZoom: 11}),
        'Topo': L.tileLayer('http://services.arcgisonline.com/ArcGIS/rest/services/USA_Topo_Maps/MapServer/tile/{z}/{y}/{x}', {maxZoom: 15}),
        'Satellite': mapboxLayer('examples.map-qfyrx5r8'),
        'Terrain': L.tileLayer('http://mt{s}.google.com/vt/lyrs=p&x={x}&y={y}&z={z}', {subdomains: '0123'}),
    }
    var layerOrder = ['Oilslick', 'Classroom', 'Terrain', 'Topo', 'Satellite'];
    L.control.layers(layers).addTo(map);

    var attr = L.control.attribution();
    attr.addTo(map);
    attr.setPrefix('\'L\' to toggle basemap');

    var activeLayer = null;
    map.on('baselayerchange', function(e) {
        activeLayer = e.layer;
    });

    var setLayer = function(tag) {
        if (activeLayer != null) {
            map.removeLayer(activeLayer);
        }
        map.addLayer(layers[tag]);
    };

    setLayer('Terrain');

    L.control.scale().addTo(map);

    $(document).keydown(function(e) {
        if (e.keyCode == 76) { // 'l'
            if (activeLayer) {
                var tag = null;
                $.each(layers, function(k, v) {
                    if (activeLayer == v) {
                        tag = k;
                        return false;
                    }
                });
                var next = layerOrder[(layerOrder.indexOf(tag) + 1) % layerOrder.length];
                setLayer(next);
            }
        }
    });

    setTimeout(function() {
        loadData(map, data);
    }, 50);
}

function singleStyle(props, highlight) {
    var style;
    if (props.type == 'peak' || props.type == 'pit') {
        style = {fillColor: '#f00'};
    } else if (props.type == 'saddle') {
        style = {fillColor: '#ff0'};
    } else if (props.type == 'threshold' || props.type == 'pthresh') {
        style = {fillColor: '#00f', radius: (props.type == 'pthresh' ? 7 : 5)};
    } else if (props.type == 'child') {
        style = {fillColor: '#0ff', radius: 8};
    } else if (props.type == 'parent') {
        style = {fillColor: '#0f0'};
    } else if (props.type == 'subsaddle') {
        style = {fillColor: '#000', radius: props.domain ? 8 : 5};
    } else if (props.type == 'childsaddle') {
        style = {fillColor: '#00f', radius: 3};
    }
    if (highlight) {
        style.color = '#ff0';
        style.weight = 3;
    } else {
        style.color = '#000';
        style.weight = 1.5;
    }
    return style;
}

CHILDREN = {};
OVERLAY = null;
function loadData(map, data) {
    var overlay = L.geoJson(data, {
        style: function(feature) {
            var props = feature.properties;
            if (props.type == 'divide') {
                return {color: '#00f'};
            } else if (props.type == 'toparent') {
                return {color: '#f00'};
            } else if (props.type == 'tochild') {
                return {color: '#a00', weight: 2, opacity: .3};
            } else if (props.type == 'domain') {
                return {color: '#0f0'};
            } else if (props.type == 'child-domain') {
                return {color: '#a0a', weight: 2};
            }
        },
        pointToLayer: function(feature, latlng) {
            if (MODE == 'single') {
                var props = feature.properties;
                var self = (props.type == 'peak' || props.type == 'pit');
                if (props.type == 'child') {
                    var m = L.marker(latlng, {icon: circledNumber(props.ix, 8, '#0ff', 1.5)});
                } else {
                    var m = L.circleMarker(latlng, singleStyle(props));
                    if (self) {
                        SELF = m;
                    }
                }
                if (props.type == 'child' || props.type == 'childsaddle') {
                    if (!CHILDREN[props.ix]) {
                        CHILDREN[props.ix] = {};
                    }
                    CHILDREN[props.ix][props.type] = m;
                }
                return m;
            } else {
                var color;
                if (feature.properties.prom_ft > 4000) {
                    color = 'hsl(0, 100%, 50%)';
                } else if (feature.properties.prom_ft > 3000) {
                    color = 'hsl(40, 100%, 50%)';
                } else if (feature.properties.prom_ft > 2000) {
                    color = 'hsl(80, 100%, 50%)';
                } else if (feature.properties.prom_ft > 1500) {
                    color = 'hsl(120, 100%, 50%)';
                } else if (feature.properties.prom_ft > 1000) {
                    color = 'hsl(160, 100%, 50%)';
                } else if (feature.properties.prom_ft > 500) {
                    color = 'hsl(200, 100%, 50%)';
                } else {
                    color = 'hsl(240, 100%, 50%)';
                }
                return L.circleMarker(latlng, {
                    radius: 10. * Math.sqrt(feature.properties.prom_ft / 500.),
                    fillColor: color,
                    fillOpacity: .5,
                });
            }
        },
        onEachFeature: function(feature, layer) {
            var props = feature.properties
            if (MODE == 'single') {                
                var $div = $('<div>');
                var self = (props.type == 'peak' || props.type == 'pit');
                if (self || props.type == 'parent' || props.type == 'child' || props.type == 'pthresh') {
                    var title = 'Peak ' + (props.name || props.geo);
                    if (!self) {
                        title = '<a target="_blank" href="/view/' + props.geo + '">' + title + '</a>';
                    } else {
                        title = '<b>' + title + '</b>';
                    }
                    var html = '';
                    if (props.type == 'child') {
                        html += '<div>Child Peak #' + props.ix + '</div>';
                    } else if (props.type == 'parent') {
                        html += '<div>Parent Peak</div>';
                    } else if (props.type == 'pthresh') {
                        html += '<div>1st Higher Peak (Prom. > 20m)</div>';
                    }
                    html += '<div>' + title + '</div>';
                    html += '<div><span style="display: inline-block; width: 3em;">Prom:</span> ' + dispdist(props, 'prom') + (props.min_bound ? '*' : '') + '</div><div><span style="display: inline-block; width: 3em;">Elev:</span> ' + dispdist(props, 'elev') + '</div>';
                    if (self) {
                        html += '<hr><div style="max-height: 150px; overflow-x: hidden; overflow-y: auto;"><table style="font-size: 12px;"><tr><td>#</td><td>Child Peak</td><td>Prominence</td></tr>';
                        _.each(DATA.features, function(e) {
                            var p = e.properties;
                            if (p.type == 'child') {
                                html += '<tr class="childentry" ix="' + p.ix + '"><td>' + p.ix + '</td><td><a target="_blank" href="/view/' + p.geo + '">' + (p.name || p.geo) + '</a></td><td>' + dispdist(p, 'prom') + '</td></tr>';
                            }
                        });
                        html += '</table></div>';
                    }
                    $div.html(html);
                } else if (props.type == 'saddle' || props.type == 'subsaddle') {
                    var html = '<div>' + (props.type == 'saddle' ? 'Key Saddle<br>(lowest point on highest path to higher ground)' : 'Secondary Saddle<br>(lowest point on another path to higher ground)') + '</div><div>' + (props.name || '') + '</div><div>Elev: ' + dispdist(props, 'elev') + '</div>';
                    if (props.type == 'subsaddle') {
                        html += (function(props) {
                            var title = props.name || props.geo;
                            title = '<a target="_blank" href="/view/' + props.geo + '">' + title + '</a>';
                            var html = '<hr><div>for ' + title + '</div>';
                            html += '<div>' + dispdist(props, 'prom') + (props.min_bound ? '*' : '') + '</div><div>' + dispdist(props, 'elev') + '</div>';
                            return html;
                        })(props.peak.properties);
                    }
                    $div.html(html);
                } else if (props.type == 'threshold') {
                    $div.html('1st higher ground');
                } else {
                    return;
                }
                layer.bindPopup($div[0]);

                var highlightChild = function(e, highlight) {
                    var ix = +$(e.currentTarget).attr('ix');
                    $.each(CHILDREN[ix], function(k, v) {
                        v.setStyle(singleStyle(v.feature.properties, highlight));
                    });
                }
                //$div.find('tr.childentry').mouseenter(function(e) { highlightChild(e, true) });
                //$div.find('tr.childentry').mouseleave(function(e) { highlightChild(e, false) });
            } else {
                var $div = $('<div>');
                $div.html('<div><a target="_blank" href="/view/' + props.geo + '">Peak ' + (props.name || props.geo) + '</a></div><div><span style="display: inline-block; width: 3em;">Prom:</span> ' + dispdist(props, 'prom') + (props.min_bound ? '*' : '') + '</div><div><span style="display: inline-block; width: 3em;">Elev:</span> ' + dispdist(props, 'elev') + '</div>');
                layer.bindPopup($div[0]);
            }
        },
    });
    overlay.addTo(map);
    
    bounds = overlay.getBounds();
    map.fitBounds(bounds);

    if (OVERLAY) {
        map.removeLayer(OVERLAY);
    }
    OVERLAY = overlay;
    SELF.openPopup();
}

function round(x, digits) {
    return Math.round(x * Math.pow(10., digits)) * Math.pow(10., -digits);
}

function dispdist(obj, field) {
    var m = obj[field + '_m'];
    var ft = obj[field + '_ft'];
    return '<span style="white-space: nowrap;">' + ft.toFixed(1) + ' ft | ' + m.toFixed(1) + ' m' + '</span>';
}

function mk_canvas(w, h) {
    var $c = $('<canvas />');
    $c.attr('width', w);
    $c.attr('height', h);
    var c = $c[0];
    var ctx = c.getContext('2d');
    return {canvas: c, context: ctx};
}

function circledNumber(n, radius, fillColor, strokeWidth) {
    var dim = Math.ceil(2 * (radius + .5 * strokeWidth + 2));
    var W = dim;
    var H = dim;
    var c = mk_canvas(W, H);

    c.context.globalAlpha = 0.5;
    c.context.beginPath();
    c.context.arc(W/2, H/2, radius, 0, 2 * Math.PI, false);
    c.context.fillStyle = fillColor;
    c.context.fill();
    c.context.lineWidth = strokeWidth;
    c.context.strokeStyle = 'black';
    c.context.stroke();

    c.context.globalAlpha = 1.;
    c.context.textAlign = 'center';
    c.context.textBaseline = 'middle';
    if (n < 10) {
        var size = 10;
    } else if (n < 100) {
        var size = 7;
    } else {
        var size = 5;
    }
    c.context.font = size + 'pt sans-serif';
    c.context.fillStyle = 'black';
    c.context.fillText(n, W/2, H/2);

    var url = c.canvas.toDataURL();
    return L.icon({iconUrl: url, iconSize: [W, H]});
}
