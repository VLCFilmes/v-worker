"""
Conversor de parâmetros entre versões v2.0 (hierárquico) e v1.0 (flat)
Para compatibilidade com N8N workflows que esperam estrutura flat.
"""

def convert_v2_to_flat(params):
    """
    Converte params v2.0 (hierárquico) para v1.0 (flat) para compatibilidade com N8N.
    
    Args:
        params (dict): Parâmetros no formato v2.0 hierárquico
    
    Returns:
        dict: Parâmetros no formato v1.0 flat
    """
    if not params or params.get('_version') != '2.0':
        # Já está em formato flat ou é inválido
        return params
    
    flat = {}
    
    # 📐 ITEM_0: VIDEO_SETTINGS
    vs = params.get('video_settings', {})
    flat['width'] = str(vs.get('width', 720))
    flat['height'] = str(vs.get('height', 1280))
    flat['fps'] = str(vs.get('fps', 30))
    flat['duration'] = str(vs.get('duration', 0))
    flat['videoWidth'] = vs.get('width', 720)
    flat['videoHeight'] = vs.get('height', 1280)
    flat['videoFps'] = vs.get('fps', 30)
    flat['videoDuration'] = vs.get('duration', 0)
    
    # 🎨 BACKGROUND
    bg = params.get('background', {})
    bg_type = bg.get('type', 'video')
    
    if bg_type == 'solid':
        flat['backgroundType'] = 'solid'
        flat['solid_background'] = 'true'
        flat['backgroundColor'] = bg.get('color', '#000000')
        flat['background_color'] = bg.get('color', '#000000')
        flat['hasBaseVideo'] = False
        flat['videoBase'] = ''
        flat['baseVideoUrls'] = []
    else:
        flat['backgroundType'] = 'video'
        flat['solid_background'] = 'false'
        flat['hasBaseVideo'] = True
        video_urls = bg.get('video_urls', [])
        flat['baseVideoUrls'] = video_urls
        flat['videoBase'] = video_urls[0] if video_urls else ''
        flat['backgroundColor'] = '#000000'
        flat['background_color'] = '#000000'
    
    # 🎬 N8N_SETTINGS
    n8n = params.get('n8n_settings', {})
    flat['n8nOverlay'] = n8n.get('overlay', False)
    flat['n8nConcatenar'] = n8n.get('concatenate', True)
    flat['n8nSilenceCut'] = n8n.get('silence_cut', True)
    flat['n8nCutMode'] = n8n.get('cut_mode', 'all_silences')
    flat['n8nQuality'] = n8n.get('quality', 17)
    flat['n8nPreset'] = n8n.get('preset', 'veryfast')
    flat['n8nUserId'] = n8n.get('user_id', 'admin')
    flat['n8nFilename'] = n8n.get('filename', '')
    flat['previewText'] = n8n.get('preview_text', '')
    
    # ✏️ ITEM_1: TEXT_STYLE
    ts = params.get('text_style', {})
    flat['fontFamily'] = ts.get('font_family', 'Montserrat Bold')
    flat['uppercase'] = str(ts.get('uppercase', True)).lower()
    flat['lineJoin'] = ts.get('line_join', 'round')
    
    # 🎨 ITEM_2: TEXT_COLOR
    tc = params.get('text_color', {})
    flat['renderType'] = tc.get('type', 'solid')
    flat['render_type'] = tc.get('type', 'solid')
    
    if tc.get('type') == 'solid':
        solid_rgb = tc.get('solid', {}).get('color_rgb', '255,255,255')
        flat['solidColorRgb'] = solid_rgb
        flat['solid_color_rgb'] = solid_rgb
    
    gradient = tc.get('gradient', {})
    flat['gradientDirection'] = gradient.get('direction', 'vertical_text')
    flat['gradient_text_direction'] = gradient.get('direction', 'vertical_text')
    flat['gradientStartRgb'] = gradient.get('start_rgb', '0,0,0')
    flat['gradientEndRgb'] = gradient.get('end_rgb', '255,255,255')
    
    # 🖌️ ITEM_3: TEXT_BORDERS (Normal)
    tb = params.get('text_borders', {})
    
    # Border 1
    b1 = tb.get('border_1', {})
    flat['border1Enabled'] = b1.get('enabled', True)
    flat['border_1_inner_enabled'] = str(b1.get('enabled', True)).lower()
    flat['border1ColorRgb'] = b1.get('color_rgb', '0,0,0')
    flat['border_1_inner_color_rgb'] = b1.get('color_rgb', '0,0,0')
    flat['border1Thickness'] = b1.get('thickness', 40)
    flat['border_1_inner_thickness'] = str(b1.get('thickness', 40))
    flat['border1Blur'] = b1.get('blur', 2)
    flat['border_1_inner_blur_radius'] = str(b1.get('blur', 2))
    flat['border1Unit'] = b1.get('unit', 'percent_font')
    
    # Border 2
    b2 = tb.get('border_2', {})
    flat['border2Enabled'] = b2.get('enabled', False)
    flat['border_2_spacing_enabled'] = str(b2.get('enabled', False)).lower()
    flat['border2ColorRgb'] = b2.get('color_rgb', '255,255,255')
    flat['border_2_spacing_color_rgb'] = b2.get('color_rgb', '255,255,255')
    flat['border2Thickness'] = b2.get('thickness', 0)
    flat['border_2_spacing_thickness'] = str(b2.get('thickness', 0))
    flat['border2Blur'] = b2.get('blur', 0)
    flat['border2Unit'] = b2.get('unit', 'percent_font')
    
    # Border 3
    b3 = tb.get('border_3', {})
    flat['border3Enabled'] = b3.get('enabled', False)
    flat['border_3_outer_enabled'] = str(b3.get('enabled', False)).lower()
    flat['border3ColorRgb'] = b3.get('color_rgb', '0,0,0')
    flat['border_3_outer_color_rgb'] = b3.get('color_rgb', '0,0,0')
    flat['border3Thickness'] = b3.get('thickness', 0)
    flat['border_3_outer_thickness'] = str(b3.get('thickness', 0))
    flat['border3Blur'] = b3.get('blur', 0)
    flat['border3Unit'] = b3.get('unit', 'px')
    
    # 🌟 ITEM_4: HIGHLIGHT_CONFIG
    hc = params.get('highlight_config', {})
    flat['enableHighlight'] = hc.get('enabled', True)
    flat['enable_highlight'] = str(hc.get('enabled', True)).lower()
    flat['syncHighlightBorders'] = hc.get('sync_borders', True)
    
    # Highlight Color
    hcc = hc.get('color', {})
    flat['highlightRenderType'] = hcc.get('type', 'solid')
    flat['highlight_render_type'] = hcc.get('type', 'solid')
    
    if hcc.get('type') == 'solid':
        h_solid_rgb = hcc.get('solid', {}).get('color_rgb', '238,255,0')
        flat['highlightSolidColorRgb'] = h_solid_rgb
        flat['highlight_solid_color_rgb'] = h_solid_rgb
    
    h_gradient = hcc.get('gradient', {})
    flat['highlightGradientDirection'] = h_gradient.get('direction', 'vertical_text')
    flat['highlightGradientStartRgb'] = h_gradient.get('start_rgb', '0,0,0')
    flat['highlightGradientEndRgb'] = h_gradient.get('end_rgb', '255,255,255')
    
    # Highlight Borders
    hb = hc.get('borders', {})
    
    # Highlight Border 1
    hb1 = hb.get('border_1', {})
    flat['highlightBorder1Enabled'] = hb1.get('enabled', True)
    flat['highlightBorder1ColorRgb'] = hb1.get('color_rgb', '0,0,0')
    flat['highlightBorder1Thickness'] = hb1.get('thickness', 40)
    flat['highlightBorder1Blur'] = hb1.get('blur', 2)
    flat['highlightBorder1Unit'] = hb1.get('unit', 'percent_font')
    
    # Highlight Border 2
    hb2 = hb.get('border_2', {})
    flat['highlightBorder2Enabled'] = hb2.get('enabled', False)
    flat['highlightBorder2ColorRgb'] = hb2.get('color_rgb', '255,255,255')
    flat['highlightBorder2Thickness'] = hb2.get('thickness', 0)
    flat['highlightBorder2Blur'] = hb2.get('blur', 0)
    flat['highlightBorder2Unit'] = hb2.get('unit', 'percent_font')
    
    # Highlight Border 3
    hb3 = hb.get('border_3', {})
    flat['highlightBorder3Enabled'] = hb3.get('enabled', False)
    flat['highlightBorder3ColorRgb'] = hb3.get('color_rgb', '0,0,0')
    flat['highlightBorder3Thickness'] = hb3.get('thickness', 0)
    flat['highlightBorder3Blur'] = hb3.get('blur', 0)
    flat['highlightBorder3Unit'] = hb3.get('unit', 'px')
    
    # 📏 ITEM_5: FONT_SIZES
    fs = params.get('font_sizes', {})
    flat['fontSize'] = fs.get('base_size', 2)
    flat['fontPhraseSize'] = fs.get('phrase_size', 25)
    flat['phrase_size'] = str(fs.get('phrase_size', 25))
    flat['fontSingleWordSize'] = fs.get('single_word_size', 50)
    flat['single_word_size'] = str(fs.get('single_word_size', 50))
    flat['fontDoubleWordSize'] = fs.get('double_word_size', 32)
    flat['double_word_size'] = str(fs.get('double_word_size', 32))
    flat['fontEmphasisSize'] = fs.get('emphasis_size', 60)
    flat['emphasis_size'] = str(fs.get('emphasis_size', 60))
    
    # 📦 ITEM_6: MARGINS
    mg = params.get('margins', {})
    flat['marginsEnabled'] = mg.get('enabled', False)
    flat['margins_enabled'] = str(mg.get('enabled', False)).lower()
    flat['marginsTop'] = mg.get('top', 0.1)
    flat['margin_top'] = str(mg.get('top', 0.1))
    flat['marginsBottom'] = mg.get('bottom', 0.2)
    flat['margin_bottom'] = str(mg.get('bottom', 0.2))
    flat['marginsLeft'] = mg.get('left', 0.1)
    flat['margin_left'] = str(mg.get('left', 0.1))
    flat['marginsRight'] = mg.get('right', 0.1)
    flat['margin_right'] = str(mg.get('right', 0.1))
    flat['marginsAutoCenter'] = mg.get('auto_center', True)
    flat['marginsAutoCenterX'] = mg.get('auto_center_x', True)
    flat['auto_center_x'] = str(mg.get('auto_center_x', True)).lower()
    flat['marginsAutoCenterY'] = mg.get('auto_center_y', True)
    flat['auto_center_y'] = str(mg.get('auto_center_y', True)).lower()
    
    # 🎭 ITEM_7: ANIMATIONS
    anims = params.get('animations', {})
    
    # Text Animation
    ta = anims.get('text', {})
    flat['textAnimationEnabled'] = ta.get('enabled', False)
    flat['textAnimationExitEnabled'] = ta.get('exit_enabled', True)
    flat['textAnimationTypeIn'] = ta.get('type_in', 'fade-scale-in')
    flat['textAnimationTypeMiddle'] = ta.get('type_middle', 'pulse')
    flat['textAnimationTypeOut'] = ta.get('type_out', 'fade-out')
    flat['textAnimationInDuration'] = ta.get('in_duration', 10)
    flat['textAnimationMiddleEnabled'] = ta.get('middle_enabled', False)
    flat['textAnimationOutDuration'] = ta.get('out_duration', 8)
    
    # Highlight Animation
    ha = anims.get('highlight', {})
    flat['highlightAnimationEnabled'] = ha.get('enabled', False)
    flat['highlightAnimationExitEnabled'] = ha.get('exit_enabled', True)
    flat['highlightAnimationTypeIn'] = ha.get('type_in', 'fade-elastic-in')
    flat['highlightAnimationTypeMiddle'] = ha.get('type_middle', 'pulse')
    flat['highlightAnimationTypeOut'] = ha.get('type_out', 'fade-out')
    flat['highlightAnimationInDuration'] = ha.get('in_duration', 12)
    flat['highlightAnimationMiddleEnabled'] = ha.get('middle_enabled', True)
    flat['highlightAnimationOutDuration'] = ha.get('out_duration', 8)
    
    # Word Backgrounds
    wb = anims.get('word_bgs', {})
    flat['wordBgsEnabled'] = wb.get('enabled', True)
    flat['word_bgs_enabled'] = str(wb.get('enabled', True)).lower()
    flat['wordBgsAnimationEnabled'] = wb.get('animation_enabled', True)
    flat['word_bgs_animation_enabled'] = str(wb.get('animation_enabled', True)).lower()
    flat['wordBgsExitEnabled'] = wb.get('exit_enabled', False)
    flat['wordBgsTypeIn'] = wb.get('type_in', 'elastic-in')
    flat['word_bgs_type_in'] = wb.get('type_in', 'elastic-in')
    flat['wordBgsTypeMiddle'] = wb.get('type_middle', 'pulse')
    flat['word_bgs_type_middle'] = wb.get('type_middle', 'pulse')
    flat['wordBgsTypeOut'] = wb.get('type_out', 'fade-out')
    flat['word_bgs_type_out'] = wb.get('type_out', 'fade-out')
    flat['wordBgsInDuration'] = wb.get('in_duration', 15)
    flat['word_bgs_in_duration'] = str(wb.get('in_duration', 15))
    flat['wordBgsMiddleEnabled'] = wb.get('middle_enabled', True)
    flat['wordBgsOutDuration'] = wb.get('out_duration', 10)
    flat['word_bgs_out_duration'] = str(wb.get('out_duration', 10))
    flat['wordBgsFrequencyHz'] = wb.get('frequency_hz', 2)
    flat['word_bgs_frequency_hz'] = str(wb.get('frequency_hz', 2))
    
    # Word Background Style
    wbs = wb.get('style', {})
    flat['wordBgsBackgroundType'] = wbs.get('background_type', 'solid')
    flat['word_bgs_background_type'] = wbs.get('background_type', 'solid')
    flat['wordBgsSolidColorRgb'] = wbs.get('solid_color_rgb', '255,255,204')
    flat['word_bgs_solid_color_rgb'] = wbs.get('solid_color_rgb', '255,255,204')
    flat['wordBgsOpacity'] = wbs.get('opacity', 1)
    flat['word_bgs_opacity'] = str(wbs.get('opacity', 1))
    flat['wordBgsPaddingX'] = wbs.get('padding_x', 12)
    flat['word_bgs_padding_x_px'] = str(wbs.get('padding_x', 12))
    flat['wordBgsPaddingY'] = wbs.get('padding_y', 8)
    flat['word_bgs_padding_y_px'] = str(wbs.get('padding_y', 8))
    flat['wordBgsBorderRadiusUnit'] = wbs.get('border_radius_unit', 'px')
    flat['word_bgs_border_radius_unit'] = wbs.get('border_radius_unit', 'px')
    flat['wordBgsBorderRadiusValue'] = wbs.get('border_radius_value', 0)
    flat['word_bgs_border_radius_value'] = str(wbs.get('border_radius_value', 0))
    flat['wordBgsBorderColorRgb'] = wbs.get('border_color_rgb', '255,255,255')
    flat['word_bgs_border_color_rgb'] = wbs.get('border_color_rgb', '255,255,255')
    flat['wordBgsBorderPx'] = wbs.get('border_px', 0)
    flat['word_bgs_border_px'] = str(wbs.get('border_px', 0))
    flat['wordBgsBorderStyle'] = wbs.get('border_style', 'solid')
    flat['word_bgs_border_style'] = wbs.get('border_style', 'solid')
    flat['wordBgsBorderBehavior'] = wbs.get('border_behavior', 'bounding_box')
    flat['word_bgs_border_behavior'] = wbs.get('border_behavior', 'bounding_box')
    
    # Phrase Backgrounds
    pb = anims.get('phrase_bgs', {})
    flat['phraseBgsEnabled'] = pb.get('enabled', False)
    flat['phrase_bgs_enabled'] = str(pb.get('enabled', False)).lower()
    flat['phraseBgsAnimationEnabled'] = pb.get('animation_enabled', True)
    flat['phraseBgsExitEnabled'] = pb.get('exit_enabled', True)
    flat['phraseBgsTypeIn'] = pb.get('type_in', 'fade-in')
    flat['phraseBgsTypeMiddle'] = pb.get('type_middle', 'pulse')
    flat['phraseBgsTypeOut'] = pb.get('type_out', 'fade-out')
    flat['phraseBgsInDuration'] = pb.get('in_duration', 15)
    flat['phraseBgsMiddleEnabled'] = pb.get('middle_enabled', True)
    flat['phraseBgsOutDuration'] = pb.get('out_duration', 10)
    flat['phraseBgsFrequencyHz'] = pb.get('frequency_hz', 2)
    flat['phraseBgsRoundedRadiusPercent'] = pb.get('rounded_radius_percent', 20)
    flat['phrase_bgs_rounded_radius_percent'] = str(pb.get('rounded_radius_percent', 20))
    
    # Subtitles
    sub = anims.get('subtitles', {})
    flat['subtitlesEnabled'] = sub.get('enabled', False)
    flat['subtitles_enabled'] = str(sub.get('enabled', False)).lower()
    flat['subtitlesExitEnabled'] = sub.get('exit_enabled', True)
    flat['subtitles_exit_animation_enabled'] = str(sub.get('exit_enabled', True)).lower()
    flat['subtitlesTypeIn'] = sub.get('type_in', 'fade-in')
    flat['subtitles_type_in'] = sub.get('type_in', 'fade-in')
    flat['subtitlesTypeMiddle'] = sub.get('type_middle', 'float')
    flat['subtitles_type_middle'] = sub.get('type_middle', 'float')
    flat['subtitlesTypeOut'] = sub.get('type_out', 'fade-out')
    flat['subtitles_type_out'] = sub.get('type_out', 'fade-out')
    flat['subtitlesInDuration'] = sub.get('in_duration', 15)
    flat['subtitles_in_duration'] = str(sub.get('in_duration', 15))
    flat['subtitlesMiddleEnabled'] = sub.get('middle_enabled', True)
    flat['subtitles_middle_animation_enabled'] = str(sub.get('middle_enabled', True)).lower()
    flat['subtitlesOutDuration'] = sub.get('out_duration', 15)
    flat['subtitles_out_duration'] = str(sub.get('out_duration', 15))
    flat['subtitlesFrequencyHz'] = sub.get('frequency_hz', 0.3)
    flat['subtitles_frequency_hz'] = str(sub.get('frequency_hz', 0.3))
    flat['subtitlesAmplitudePx'] = sub.get('amplitude_px', 5)
    flat['subtitles_amplitude_px'] = str(sub.get('amplitude_px', 5))
    
    # Fullscreen Background
    fsbg = anims.get('fullscreen_bg', {})
    flat['full_screen_bg_enabled'] = str(fsbg.get('enabled', True)).lower()
    flat['full_screen_bg_style_type'] = fsbg.get('style_type', 'solid')
    flat['fullScreenBgBackgroundType'] = fsbg.get('style_type', 'solid')
    flat['full_screen_bg_solid_color_rgb'] = fsbg.get('solid_color_rgb', '0,0,0')
    flat['fullScreenBgSolidColorRgb'] = fsbg.get('solid_color_rgb', '0,0,0')
    flat['full_screen_bg_opacity'] = str(fsbg.get('opacity', 0.7))
    flat['fullScreenBgOpacity'] = fsbg.get('opacity', 0.7)
    flat['force_disable_fullscreen_bg'] = str(fsbg.get('force_disable', False)).lower()
    
    # 📐 ITEM_8: POSITIONING
    pos = params.get('positioning', {})
    flat['positioning_enabled'] = str(pos.get('enabled', False)).lower()
    flat['globalPositionEnabled'] = pos.get('enabled', False)
    
    gp = pos.get('global_position', {})
    flat['globalPositionX'] = gp.get('x', 0)
    flat['global_x'] = str(gp.get('x', 0))
    flat['globalPositionY'] = gp.get('y', 0.35)
    flat['global_y'] = str(gp.get('y', 0.35))
    
    gfp = pos.get('global_fullscreen_position', {})
    flat['globalPositionFullscreenEnabled'] = gfp.get('enabled', True)
    flat['global_position_full_screen_enabled'] = str(gfp.get('enabled', True)).lower()
    flat['globalPositionFullscreenX'] = gfp.get('x', 0)
    flat['global_fullscreen_x'] = str(gfp.get('x', 0))
    flat['globalPositionFullscreenY'] = gfp.get('y', 0)
    flat['global_fullscreen_y'] = str(gfp.get('y', 0))
    
    da = pos.get('default_anchor', {})
    flat['default_anchor_x'] = str(da.get('x', 0.5))
    flat['default_anchor_y'] = str(da.get('y', 0.5))
    
    pad = pos.get('padding', {})
    flat['paddingX'] = pad.get('x', 10)
    flat['paddingY'] = pad.get('y', 10)
    
    # 🎯 ITEM_9: INTELLIGENT_SEGMENTATION
    iseg = params.get('intelligent_segmentation', {})
    
    # Punctuation
    punct = iseg.get('punctuation', {})
    flat['punctuationEnabled'] = punct.get('enabled', True)
    flat['punctMaxWordsBeforeBreak'] = punct.get('max_words_before_break', 6)
    flat['max_words_before_punctuation_break'] = str(punct.get('max_words_before_break', 6))
    flat['punctPreferBreaks'] = punct.get('prefer_breaks', True)
    
    # Pause Detection
    pause = iseg.get('pause_detection', {})
    flat['pauseThresholdMs'] = pause.get('threshold_ms', 500)
    flat['pause_threshold_ms'] = str(pause.get('threshold_ms', 500))
    
    # Conservative Mode
    cons = iseg.get('conservative_mode', {})
    flat['conservativeModeEnabled'] = cons.get('enabled', False)
    flat['conservative_mode_enabled'] = str(cons.get('enabled', False)).lower()
    flat['conservativeUniformLength'] = cons.get('uniform_length', 4)
    flat['conservativeDisableEmphasis'] = cons.get('disable_emphasis', False)
    
    # Phrase Defaults
    pd = iseg.get('phrase_defaults', {})
    flat['defaultPhraseMinWords'] = pd.get('min_words', 1)
    flat['default_phrase_min_words'] = str(pd.get('min_words', 1))
    flat['defaultPhraseMaxWords'] = pd.get('max_words', 4)
    flat['default_phrase_max_words'] = str(pd.get('max_words', 4))
    flat['averagePhraseLength'] = pd.get('average_length', 2)
    flat['average_phrase_length'] = str(pd.get('average_length', 2))
    
    # Emphasis
    emph = iseg.get('emphasis', {})
    flat['emphasisWithoutBgEnabled'] = emph.get('without_bg_enabled', False)
    flat['emphasis_without_bg_enabled'] = str(emph.get('without_bg_enabled', False)).lower()
    flat['emphasisWithoutBgForceDisable'] = emph.get('without_bg_force_disable', False)
    
    # 🎬 ITEM_10: FULLSCREEN_CONTROLS
    fsc = params.get('fullscreen_controls', {})
    flat['fullscreenControlsEnabled'] = fsc.get('enabled', True)
    flat['globalDisableFullscreen'] = fsc.get('global_disable', False)
    
    # Priority
    prio = fsc.get('priority', {})
    flat['fullscreenPriorityEmphasis'] = prio.get('emphasis', True)
    flat['priority_emphasis'] = str(prio.get('emphasis', True)).lower()
    flat['fullscreenPriorityInterjections'] = prio.get('interjections', True)
    flat['priority_interjections'] = str(prio.get('interjections', True)).lower()
    flat['fullscreenPriorityQuestions'] = prio.get('questions', False)
    flat['priority_questions'] = str(prio.get('questions', False)).lower()
    flat['fullscreenNegativeWordsEnabled'] = prio.get('negative_words_enabled', True)
    flat['negative_words_enabled'] = str(prio.get('negative_words_enabled', True)).lower()
    
    # Limits
    lim = fsc.get('limits', {})
    flat['fullscreenMaxPercentage'] = lim.get('max_percentage', 0.35)
    flat['max_fullscreen_percentage'] = str(lim.get('max_percentage', 0.35))
    flat['fullscreenMaxConsecutive'] = lim.get('max_consecutive', 2)
    flat['max_consecutive_fullscreen'] = str(lim.get('max_consecutive', 2))
    flat['fullscreenMinPhrasesBetween'] = lim.get('min_phrases_between', 2)
    flat['fullscreenAntiPingPong'] = lim.get('anti_ping_pong', False)
    
    # Min Duration
    mindur = fsc.get('min_duration', {})
    flat['fullscreenMinDurationEnabled'] = mindur.get('enabled', True)
    flat['min_duration_enabled'] = str(mindur.get('enabled', True)).lower()
    flat['fullscreenMinDurationMs'] = mindur.get('ms', 500)
    flat['min_duration_ms'] = str(mindur.get('ms', 500))
    flat['fullscreenMergePhrases'] = mindur.get('merge_phrases', True)
    flat['merge_phrases_for_min_duration'] = str(mindur.get('merge_phrases', True)).lower()
    
    # Duration Threshold
    dthresh = fsc.get('duration_threshold', {})
    flat['durationThresholdMs'] = dthresh.get('ms', 800)
    flat['duration_threshold_ms'] = str(dthresh.get('ms', 800))
    
    # 🖼️ ITEM_11: PNG_SHADOW
    shadow = params.get('png_shadow', {})
    flat['pngShadowEnabled'] = shadow.get('enabled', True)
    
    # Mantém textColor se existir (não está no JSON Schema v2.0 oficial)
    if 'textColor' in params:
        flat['textColor'] = params['textColor']
    
    # Mantém _version para referência
    flat['_version'] = '1.0'
    flat['_converted_from'] = '2.0'
    
    return flat

