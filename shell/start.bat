@echo off

set input1=%1%
set input2=%2%

echo 参数 %input1% %input2%
call :executeProgram %input1%
goto:EOF

:executeProgram
    if "%~1"=="all" (
        call :execute "idea"
    )else (
        call :execute %~1
    )
    
:execute
    if "%~1"=="idea" (
        echo "启动IDEA"
        cd D:\idea\bin\
        start idea64.exe
        cd D:\script\
    )
    goto:EOF
