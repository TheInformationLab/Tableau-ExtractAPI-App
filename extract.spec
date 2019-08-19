# -*- mode: python -*-

block_cipher = None

options = [ ('u', None, 'OPTION')]
a = Analysis(['extract.py'],
             pathex=['/Users/craig/Documents/Github/tableau-extractAPI-app'],
             binaries=[],
             datas=[('./tableauhyperapi/', './tableauhyperapi')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          options,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='extract',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )
