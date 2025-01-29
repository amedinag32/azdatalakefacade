upload:
	# Verificar que el paquete se genera sin errores
	python3 setup.py sdist bdist_wheel

	# Validar el paquete generado
	twine check dist/*

	# subir
	twine upload dist/*


