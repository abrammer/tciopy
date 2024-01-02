{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}


   {% block attributes %}
   {% if attributes %}

   .. autosummary::

   {% endif %}
   {% endblock %}


   {% block methods %}

   {% if methods %}
   .. rubric:: {{ _('Data Columns:') }}

   .. autosummary::
   {% endif %}
   {% endblock %}
